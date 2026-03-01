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

  action_fallback StarciteWeb.FallbackController

  @default_list_limit 100
  @max_list_limit 1000

  @doc """
  Create a session.
  """
  def create(conn, params) do
    with {:ok, auth} <- fetch_auth(conn),
         {:ok, opts} <- validate_create(params, auth),
         {:ok, session} <- WritePath.create_session(opts) do
      :ok = Telemetry.ingest_edge(:create_session, auth.principal.tenant_id, :ok, :none)

      conn
      |> put_status(:created)
      |> json(session)
    else
      error ->
        :ok = maybe_emit_ingest_edge_error(:create_session, conn, error)
        error
    end
  end

  @doc """
  Append one event to a session.
  """
  def append(conn, %{"id" => id} = params) do
    measure_append_request(fn ->
      with {:ok, auth} <- fetch_auth(conn),
           :ok <- Policy.allowed_to_access_session(auth, id),
           :ok <- authorize_append(auth, id),
           {:ok, event, expected_seq} <- validate_append(params, auth),
           {:ok, reply} <- append_event(id, event, expected_seq) do
        :ok = Telemetry.ingest_edge(:append_event, auth.principal.tenant_id, :ok, :none)

        conn
        |> put_status(:created)
        |> json(reply)
      else
        error ->
          :ok = maybe_emit_ingest_edge_error(:append_event, conn, error)
          error
      end
    end)
  end

  def append(_conn, _params), do: {:error, :invalid_event}

  defp append_event(id, event, nil), do: WritePath.append_event(id, event)

  defp append_event(id, event, expected_seq),
    do: WritePath.append_event(id, event, expected_seq: expected_seq)

  defp maybe_emit_ingest_edge_error(operation, %Plug.Conn{assigns: assigns}, result)
       when operation in [:create_session, :append_event] and is_map(assigns) do
    case assigns do
      %{auth: %Context{principal: %{tenant_id: tenant_id}}}
      when is_binary(tenant_id) and tenant_id != "" ->
        Telemetry.ingest_edge(operation, tenant_id, :error, ingest_edge_error_reason(result))

      _ ->
        :ok
    end
  end

  defp ingest_edge_error_reason({:error, {:not_leader, _leader}}), do: :not_leader
  defp ingest_edge_error_reason({:error, :not_leader}), do: :not_leader

  defp ingest_edge_error_reason({:error, {:expected_seq_conflict, _expected, _current}}),
    do: :seq_conflict

  defp ingest_edge_error_reason({:error, {:expected_seq_conflict, _current}}), do: :seq_conflict

  defp ingest_edge_error_reason(
         {:error, {:producer_seq_conflict, _producer_id, _expected, _got}}
       ),
       do: :seq_conflict

  defp ingest_edge_error_reason({:error, :producer_replay_conflict}), do: :seq_conflict
  defp ingest_edge_error_reason({:timeout, _leader}), do: :timeout
  defp ingest_edge_error_reason({:error, {:timeout, _leader}}), do: :timeout
  defp ingest_edge_error_reason({:error, {:no_available_replicas, _failures}}), do: :unavailable

  defp ingest_edge_error_reason({:error, reason})
       when reason in [:archive_read_unavailable, :event_gap_detected, :event_store_backpressure],
       do: :unavailable

  defp ingest_edge_error_reason({:error, :unauthorized}), do: :unauthorized

  defp ingest_edge_error_reason({:error, reason})
       when reason in [:forbidden, :forbidden_scope, :forbidden_session, :forbidden_tenant],
       do: :forbidden

  defp ingest_edge_error_reason({:error, :session_not_found}), do: :session_not_found
  defp ingest_edge_error_reason({:error, :session_exists}), do: :session_exists

  defp ingest_edge_error_reason({:error, reason})
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

  defp ingest_edge_error_reason({:error, _reason}), do: :internal

  defp authorize_append(%Context{} = auth, id) when is_binary(id) and id != "" do
    with {:ok, session} <- ReadPath.get_session(id),
         :ok <- Policy.allowed_to_append_session(auth, session) do
      :ok
    end
  end

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

  defp validate_append(
         %{
           "type" => type,
           "payload" => payload,
           "producer_id" => producer_id,
           "producer_seq" => producer_seq,
           "source" => source,
           "metadata" => metadata
         } = params,
         %Context{} = auth
       )
       when is_binary(type) and type != "" and is_map(payload) and is_binary(producer_id) and
              producer_id != "" and is_integer(producer_seq) and producer_seq > 0 and
              is_binary(source) and source != "" and is_map(metadata) do
    case {Map.get(params, "refs"), Map.get(params, "idempotency_key"),
          Map.get(params, "expected_seq")} do
      {nil, nil, nil} ->
        with {:ok, actor} <- Policy.resolve_event_actor(auth, params["actor"]) do
          event = %{
            type: type,
            payload: payload,
            actor: actor,
            source: source,
            metadata: Policy.attach_principal_metadata(auth, metadata),
            refs: %{},
            idempotency_key: nil,
            producer_id: producer_id,
            producer_seq: producer_seq
          }

          {:ok, event, nil}
        end

      _other ->
        validate_append_slow(params, auth)
    end
  end

  defp validate_append(
         %{
           "type" => type,
           "payload" => payload
         } = params,
         %Context{} = auth
       )
       when is_binary(type) and type != "" and is_map(payload) do
    validate_append_slow(params, auth)
  end

  defp validate_append(_params, _auth), do: {:error, :invalid_event}

  defp validate_append_slow(
         %{
           "type" => type,
           "payload" => payload,
           "producer_id" => producer_id,
           "producer_seq" => producer_seq
         } = params,
         %Context{} = auth
       )
       when is_binary(type) and type != "" and is_map(payload) do
    with {:ok, validated_producer_id} <- required_non_empty_string(producer_id),
         {:ok, validated_producer_seq} <- required_positive_integer(producer_seq),
         {:ok, actor} <- Policy.resolve_event_actor(auth, params["actor"]),
         {:ok, source} <- optional_non_empty_string(params["source"]),
         {:ok, metadata} <- optional_object(params["metadata"]),
         {:ok, refs} <- optional_refs(params["refs"]),
         {:ok, idempotency_key} <- optional_non_empty_string(params["idempotency_key"]),
         {:ok, expected_seq} <- optional_non_neg_integer(params["expected_seq"]) do
      metadata = Policy.attach_principal_metadata(auth, metadata)

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

  defp validate_append_slow(_params, _auth), do: {:error, :invalid_event}

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
