defmodule StarciteWeb.SessionController do
  @moduledoc """
  HTTP controller for Starcite session primitives:

  - `create` via `POST /v1/sessions`
  - `append` via `POST /v1/sessions/:id/append`
  - `index` via `GET /v1/sessions`
  """

  use StarciteWeb, :controller

  alias Starcite.{ReadPath, WritePath}
  alias Starcite.Observability.Telemetry
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.Policy
  alias StarciteWeb.SessionAppend

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
              ingest_edge_error_reason(error)
            )

          error
      end
    end
  end

  @doc """
  Append one event to a session.
  """
  def append(conn, %{"id" => id} = params) do
    with {:ok, auth} <- fetch_auth(conn) do
      measure_append_request(fn ->
        with :ok <- Policy.allowed_to_access_session(auth, id),
             {:ok, session} <- ReadPath.get_session(id),
             {:ok, reply} <- SessionAppend.append(id, params, auth, session) do
          :ok = Telemetry.ingest_edge(:append_event, auth.principal.tenant_id, :ok)

          conn
          |> put_status(:created)
          |> json(reply)
        else
          error ->
            :ok =
              Telemetry.ingest_edge(
                :append_event,
                auth.principal.tenant_id,
                :error,
                ingest_edge_error_reason(error)
              )

            error
        end
      end)
    end
  end

  def append(_conn, _params), do: {:error, :invalid_event}

  def ingest_edge_error_reason({:error, {:not_leader, _leader}}), do: :not_leader
  def ingest_edge_error_reason({:error, :not_leader}), do: :not_leader

  def ingest_edge_error_reason({:error, {:expected_seq_conflict, _expected, _current}}),
    do: :seq_conflict

  def ingest_edge_error_reason({:error, {:expected_seq_conflict, _current}}), do: :seq_conflict

  def ingest_edge_error_reason({:error, {:producer_seq_conflict, _producer_id, _expected, _got}}),
    do: :seq_conflict

  def ingest_edge_error_reason({:error, :producer_replay_conflict}), do: :seq_conflict
  def ingest_edge_error_reason({:timeout, _leader}), do: :timeout
  def ingest_edge_error_reason({:error, {:timeout, _leader}}), do: :timeout
  def ingest_edge_error_reason({:error, {:no_available_replicas, _failures}}), do: :unavailable

  def ingest_edge_error_reason({:error, reason})
      when reason in [:archive_read_unavailable, :event_gap_detected, :event_store_backpressure],
      do: :unavailable

  def ingest_edge_error_reason({:error, :unauthorized}), do: :unauthorized

  def ingest_edge_error_reason({:error, reason})
      when reason in [:forbidden, :forbidden_scope, :forbidden_session, :forbidden_tenant],
      do: :forbidden

  def ingest_edge_error_reason({:error, :session_not_found}), do: :session_not_found
  def ingest_edge_error_reason({:error, :session_exists}), do: :session_exists

  def ingest_edge_error_reason({:error, reason})
      when reason in [
             :invalid_event,
             :invalid_metadata,
             :invalid_refs,
             :invalid_cursor,
             :invalid_tail_batch_size,
             :invalid_limit,
             :invalid_list_query,
             :invalid_write_node,
             :invalid_group_id,
             :invalid_websocket_upgrade,
             :invalid_session,
             :invalid_session_id
           ],
      do: :invalid_request

  def ingest_edge_error_reason({:error, reason}) when is_atom(reason), do: reason

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
    with {:ok, %Starcite.Auth.Principal{} = creator_principal} <-
           Policy.can_create_session(auth, params),
         {:ok, requested_id} <- optional_non_empty_string(params["id"]),
         {:ok, id} <- Policy.resolve_create_session_id(auth, requested_id),
         {:ok, title} <- optional_string(params["title"]),
         {:ok, metadata} <- optional_object(params["metadata"]) do
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
    with {:ok, limit} <- optional_limit(params["limit"]),
         {:ok, cursor} <- optional_cursor(params["cursor"]),
         {:ok, metadata} <- optional_metadata_filters(params) do
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

  defp optional_non_empty_string(nil), do: {:ok, nil}
  defp optional_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp optional_non_empty_string(_value), do: {:error, :invalid_event}

  defp optional_object(nil), do: {:ok, %{}}
  defp optional_object(value) when is_map(value) and not is_list(value), do: {:ok, value}
  defp optional_object(_value), do: {:error, :invalid_metadata}

  defp optional_string(nil), do: {:ok, nil}
  defp optional_string(value) when is_binary(value), do: {:ok, value}
  defp optional_string(_value), do: {:error, :invalid_event}

  defp optional_limit(nil), do: {:ok, @default_list_limit}

  defp optional_limit(value) when is_integer(value) and value > 0 and value <= @max_list_limit,
    do: {:ok, value}

  defp optional_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 and parsed <= @max_list_limit -> {:ok, parsed}
      _ -> {:error, :invalid_limit}
    end
  end

  defp optional_limit(_value), do: {:error, :invalid_limit}

  defp optional_cursor(nil), do: {:ok, nil}
  defp optional_cursor(value) when is_binary(value) and value != "", do: {:ok, value}
  defp optional_cursor(_value), do: {:error, :invalid_cursor}

  defp optional_metadata_filters(params) when is_map(params) do
    with {:ok, nested} <- optional_metadata_filter_map(params["metadata"]) do
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

  defp optional_metadata_filter_map(nil), do: {:ok, %{}}

  defp optional_metadata_filter_map(value) when is_map(value) do
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

  defp optional_metadata_filter_map(_value), do: {:error, :invalid_metadata}

  defp metadata_filter_value(value)
       when is_binary(value) or is_boolean(value) or is_number(value),
       do: {:ok, value}

  defp metadata_filter_value(_value), do: :error

  defp measure_append_request(fun) when is_function(fun, 0) do
    started_at = System.monotonic_time()
    result = fun.()
    duration_ms = elapsed_ms_since(started_at)
    :ok = Telemetry.request(:append_event, :ack, request_outcome(result), duration_ms)
    result
  end

  defp request_outcome(%Plug.Conn{}), do: :ok
  defp request_outcome({:timeout, _reason}), do: :timeout
  defp request_outcome(_result), do: :error

  defp elapsed_ms_since(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end
end
