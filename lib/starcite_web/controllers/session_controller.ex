defmodule StarciteWeb.SessionController do
  @moduledoc """
  HTTP controller for Starcite session primitives:

  - `create` via `POST /v1/sessions`
  - `append` via `POST /v1/sessions/:id/append`
  - `index` via `GET /v1/sessions`
  """

  use StarciteWeb, :controller

  alias Starcite.WritePath
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.Policy
  alias StarciteWeb.ErrorInfo
  alias StarciteWeb.SessionAppend
  alias Starcite.Observability.Telemetry

  action_fallback StarciteWeb.FallbackController

  @default_list_limit 100
  @max_list_limit 1000

  @doc """
  Create a session.
  """
  def create(conn, params) do
    with {:ok, auth} <- fetch_auth(conn) do
      with {:ok, opts} <- validate_create(params, auth),
           {:ok, session} <- WritePath.create_session(opts) do
        :ok = Telemetry.ingest_edge(:create_session, auth.principal.tenant_id, :ok)

        conn
        |> put_status(:created)
        |> json(session)
      else
        error ->
          :ok =
            Telemetry.ingest_edge(
              :create_session,
              auth.principal.tenant_id,
              :error,
              ErrorInfo.ingest_edge_reason(error)
            )

          error
      end
    end
  end

  @doc """
  Append one event to a session.
  """
  def append(conn, %{"id" => id} = params) do
    with {:ok, auth} <- fetch_auth(conn),
         {:ok, reply} <- SessionAppend.append(auth, id, params) do
      conn
      |> put_status(:created)
      |> json(reply)
    end
  end

  def append(_conn, _params), do: {:error, :invalid_event}

  @doc """
  List known sessions from the configured archive adapter.
  """
  def index(conn, params) do
    with {:ok, auth} <- fetch_auth(conn),
         {:ok, opts} <- validate_list(params),
         :ok <- Policy.can_list_sessions(auth),
         {:ok, page} <- list_sessions(auth, opts) do
      json(conn, page)
    end
  end

  # Validation

  defp validate_create(params, %Context{} = auth) when is_map(params) do
    requested_id = Map.get(params, "id")
    title = Map.get(params, "title")
    metadata = Map.get(params, "metadata", %{})

    with {:ok, %Starcite.Auth.Principal{} = creator_principal} <-
           Policy.can_create_session(auth, params),
         :ok <- validate_optional_non_empty_string(requested_id),
         {:ok, id} <- Policy.resolve_create_session_id(auth, requested_id),
         :ok <- validate_optional_string(title),
         {:ok, metadata} <- validate_metadata(metadata) do
      {:ok,
       [
         id: id,
         title: title,
         creator_principal: creator_principal,
         tenant_id: creator_principal.tenant_id,
         metadata: metadata
       ]}
    end
  end

  defp validate_create(_params, _auth), do: {:error, :invalid_session}

  defp validate_list(params) when is_map(params) do
    with {:ok, limit} <- parse_limit(Map.get(params, "limit", @default_list_limit)),
         {:ok, cursor} <- parse_cursor(Map.get(params, "cursor")),
         {:ok, metadata} <- parse_metadata_filters(params, Map.get(params, "metadata", %{})) do
      {:ok, %{limit: limit, cursor: cursor, metadata: metadata}}
    end
  end

  defp validate_list(_params), do: {:error, :invalid_list_query}

  defp list_sessions(%Context{kind: :none}, opts) when is_map(opts) do
    Starcite.Archive.Store.list_sessions(opts)
  end

  defp list_sessions(
         %Context{
           kind: :jwt,
           principal: %Starcite.Auth.Principal{tenant_id: tenant_id},
           session_id: nil
         },
         opts
       )
       when is_binary(tenant_id) and tenant_id != "" and is_map(opts) do
    Starcite.Archive.Store.list_sessions(
      opts
      |> Map.put(:tenant_id, tenant_id)
    )
  end

  defp list_sessions(
         %Context{
           kind: :jwt,
           principal: %Starcite.Auth.Principal{tenant_id: tenant_id},
           session_id: session_id
         },
         opts
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(session_id) and
              session_id != "" and is_map(opts) do
    Starcite.Archive.Store.list_sessions_by_ids(
      [session_id],
      opts
      |> Map.put(:tenant_id, tenant_id)
    )
  end

  defp list_sessions(_auth, _opts), do: {:error, :forbidden}

  defp fetch_auth(%Plug.Conn{assigns: %{auth: %Context{} = auth}}), do: {:ok, auth}
  defp fetch_auth(_conn), do: {:error, :unauthorized}

  defp validate_optional_non_empty_string(nil), do: :ok
  defp validate_optional_non_empty_string(value) when is_binary(value) and value != "", do: :ok
  defp validate_optional_non_empty_string(_value), do: {:error, :invalid_event}

  defp validate_metadata(%{} = metadata), do: {:ok, metadata}
  defp validate_metadata(_value), do: {:error, :invalid_metadata}

  defp validate_optional_string(nil), do: :ok
  defp validate_optional_string(value) when is_binary(value), do: :ok
  defp validate_optional_string(_value), do: {:error, :invalid_event}

  defp parse_limit(value) when is_integer(value) and value > 0 and value <= @max_list_limit,
    do: {:ok, value}

  defp parse_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 and parsed <= @max_list_limit -> {:ok, parsed}
      _ -> {:error, :invalid_limit}
    end
  end

  defp parse_limit(_value), do: {:error, :invalid_limit}

  defp parse_cursor(nil), do: {:ok, nil}
  defp parse_cursor(value) when is_binary(value) and value != "", do: {:ok, value}
  defp parse_cursor(_value), do: {:error, :invalid_cursor}

  defp parse_metadata_filters(params, %{} = nested) when is_map(params) do
    with {:ok, nested} <- validate_metadata_filters(nested) do
      dotted =
        Enum.reduce(params, %{}, fn
          {<<"metadata.", key::binary>>, value}, acc when key != "" ->
            case metadata_filter_value(value) do
              {:ok, normalized} -> Map.put(acc, key, normalized)
              :error -> acc
            end

          _, acc ->
            acc
        end)

      {:ok, Map.merge(nested, dotted)}
    end
  end

  defp parse_metadata_filters(_params, _metadata), do: {:error, :invalid_metadata}

  defp validate_metadata_filters(value) when is_map(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn
      {key, entry}, {:ok, acc} when is_binary(key) and key != "" ->
        case metadata_filter_value(entry) do
          {:ok, normalized} -> {:cont, {:ok, Map.put(acc, key, normalized)}}
          :error -> {:halt, {:error, :invalid_metadata}}
        end

      _, _ ->
        {:halt, {:error, :invalid_metadata}}
    end)
  end

  defp metadata_filter_value(value)
       when is_binary(value) or is_boolean(value) or is_number(value),
       do: {:ok, value}

  defp metadata_filter_value(_value), do: :error
end
