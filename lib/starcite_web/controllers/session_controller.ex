defmodule StarciteWeb.SessionController do
  @moduledoc """
  HTTP controller for Starcite session primitives:

  - `create` via `POST /v1/sessions`
  - `append` via `POST /v1/sessions/:id/append`
  - `index` via `GET /v1/sessions`
  """

  use StarciteWeb, :controller

  alias Starcite.Runtime
  alias StarciteWeb.Auth.Policy

  action_fallback StarciteWeb.FallbackController

  @default_list_limit 100
  @max_list_limit 1000

  @doc """
  Create a session.
  """
  def create(conn, params) do
    auth = conn.assigns[:auth] || %{kind: :none}

    with {:ok, opts} <- validate_create(params, auth),
         {:ok, session} <- Runtime.create_session(opts) do
      conn
      |> put_status(:created)
      |> json(session)
    end
  end

  @doc """
  Append one event to a session.
  """
  def append(conn, %{"id" => id} = params) do
    auth = conn.assigns[:auth] || %{kind: :none}

    with :ok <- Policy.authorize_session_reference(auth, id),
         {:ok, session} <- Runtime.get_session(id),
         :ok <- Policy.authorize_append(auth, session),
         {:ok, event, expected_seq} <- validate_append(params, auth),
         {:ok, reply} <- Runtime.append_event(id, event, expected_seq: expected_seq) do
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
    auth = conn.assigns[:auth] || %{kind: :none}

    with {:ok, opts} <- validate_list(params),
         {:ok, scope} <- Policy.authorize_list_sessions(auth),
         {:ok, page} <- list_sessions(scope, opts) do
      json(conn, page)
    end
  end

  # Validation

  defp validate_create(params, auth) when is_map(params) and is_map(auth) do
    with {:ok, creator_principal} <- Policy.authorize_create_session(auth, params),
         {:ok, id} <- optional_non_empty_string(params["id"]),
         {:ok, title} <- optional_string(params["title"]),
         {:ok, metadata} <- optional_object(params["metadata"]) do
      {:ok,
       [
         id: id,
         title: title,
         creator_principal: creator_principal,
         metadata: metadata
       ]}
    end
  end

  defp validate_create(_params, _auth), do: {:error, :invalid_session}

  defp validate_append(
         %{
           "type" => type,
           "payload" => payload,
           "producer_id" => producer_id,
           "producer_seq" => producer_seq
         } = params,
         auth
       )
       when is_binary(type) and type != "" and is_map(payload) and is_map(auth) do
    with {:ok, validated_producer_id} <- required_non_empty_string(producer_id),
         {:ok, validated_producer_seq} <- required_positive_integer(producer_seq),
         {:ok, actor} <- Policy.resolve_actor(auth, params["actor"]),
         {:ok, source} <- optional_non_empty_string(params["source"]),
         {:ok, metadata} <- optional_object(params["metadata"]),
         {:ok, refs} <- optional_refs(params["refs"]),
         {:ok, idempotency_key} <- optional_non_empty_string(params["idempotency_key"]),
         {:ok, expected_seq} <- optional_non_neg_integer(params["expected_seq"]) do
      metadata = Policy.stamp_event_metadata(auth, metadata)

      event = %{
        type: type,
        payload: payload,
        actor: actor,
        source: source,
        metadata: metadata,
        refs: refs,
        idempotency_key: idempotency_key,
        producer_id: validated_producer_id,
        producer_seq: validated_producer_seq
      }

      {:ok, event, expected_seq}
    end
  end

  defp validate_append(_params, _auth), do: {:error, :invalid_event}

  defp validate_list(params) when is_map(params) do
    with {:ok, limit} <- optional_limit(params["limit"]),
         {:ok, cursor} <- optional_cursor(params["cursor"]),
         {:ok, metadata} <- optional_metadata_filters(params) do
      {:ok, %{limit: limit, cursor: cursor, metadata: metadata}}
    end
  end

  defp validate_list(_params), do: {:error, :invalid_list_query}

  defp list_sessions(:all, opts) when is_map(opts) do
    Starcite.Archive.Store.list_sessions(opts)
  end

  defp list_sessions(
         %{tenant_id: tenant_id, owner_principal_ids: owner_principal_ids},
         opts
       )
       when is_binary(tenant_id) and tenant_id != "" and is_list(owner_principal_ids) and
              is_map(opts) do
    Starcite.Archive.Store.list_sessions(
      opts
      |> Map.put(:tenant_id, tenant_id)
      |> Map.put(:owner_principal_ids, owner_principal_ids)
    )
  end

  defp list_sessions(_scope, _opts), do: {:error, :forbidden}

  defp required_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_non_empty_string(_value), do: {:error, :invalid_event}

  defp required_positive_integer(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp required_positive_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> {:ok, parsed}
      _ -> {:error, :invalid_event}
    end
  end

  defp required_positive_integer(_value), do: {:error, :invalid_event}

  defp optional_non_empty_string(nil), do: {:ok, nil}
  defp optional_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp optional_non_empty_string(_value), do: {:error, :invalid_event}

  defp optional_object(nil), do: {:ok, %{}}
  defp optional_object(value) when is_map(value) and not is_list(value), do: {:ok, value}
  defp optional_object(_value), do: {:error, :invalid_metadata}

  defp optional_refs(nil), do: {:ok, %{}}

  defp optional_refs(refs) when is_map(refs) and not is_list(refs) do
    with {:ok, _} <- optional_non_neg_integer(refs["to_seq"]),
         {:ok, _} <- optional_string(refs["request_id"]),
         {:ok, _} <- optional_string(refs["sequence_id"]),
         {:ok, _} <- optional_non_neg_integer(refs["step"]) do
      {:ok, refs}
    end
  end

  defp optional_refs(_value), do: {:error, :invalid_refs}

  defp optional_string(nil), do: {:ok, nil}
  defp optional_string(value) when is_binary(value), do: {:ok, value}
  defp optional_string(_value), do: {:error, :invalid_event}

  defp optional_non_neg_integer(nil), do: {:ok, nil}
  defp optional_non_neg_integer(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp optional_non_neg_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, :invalid_event}
    end
  end

  defp optional_non_neg_integer(_value), do: {:error, :invalid_event}

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
end
