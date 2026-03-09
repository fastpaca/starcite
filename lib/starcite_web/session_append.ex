defmodule StarciteWeb.SessionAppend do
  @moduledoc false

  alias Starcite.Observability.Telemetry
  alias Starcite.{ReadPath, Session, WritePath}
  alias StarciteWeb.Auth.{Context, Policy}
  alias StarciteWeb.ErrorInfo

  @spec append(Context.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def append(%Context{} = auth, session_id, params)
      when is_binary(session_id) and session_id != "" and is_map(params) do
    measure_append_request(fn ->
      with {:ok, _session} <- authorize_append(auth, session_id),
           {:ok, event, expected_seq} <- validate(params, auth),
           {:ok, reply} <- append_event(session_id, event, expected_seq) do
        :ok = Telemetry.ingest_edge(:append_event, auth.principal.tenant_id, :ok)
        {:ok, reply}
      else
        error ->
          :ok =
            Telemetry.ingest_edge(
              :append_event,
              auth.principal.tenant_id,
              :error,
              ErrorInfo.ingest_edge_reason(error)
            )

          error
      end
    end)
  end

  def append(_auth, _session_id, _params), do: {:error, :invalid_event}

  defp authorize_append(%Context{} = auth, session_id)
       when is_binary(session_id) and session_id != "" do
    with :ok <- Policy.allowed_to_access_session(auth, session_id),
         {:ok, %Session{} = session} <- ReadPath.get_session(session_id),
         :ok <- Policy.allowed_to_append_session(auth, session) do
      {:ok, session}
    end
  end

  defp authorize_append(_auth, _session_id), do: {:error, :invalid_session_id}

  defp append_event(session_id, event, nil), do: WritePath.append_event(session_id, event)

  defp append_event(session_id, event, expected_seq) do
    WritePath.append_event(session_id, event, expected_seq: expected_seq)
  end

  defp validate(
         %{
           "type" => type,
           "payload" => payload,
           "producer_id" => producer_id,
           "producer_seq" => producer_seq
         } = params,
         %Context{} = auth
       )
       when is_binary(type) and type != "" and is_map(payload) do
    source = Map.get(params, "source")
    metadata = Map.get(params, "metadata", %{})
    refs = Map.get(params, "refs", %{})
    idempotency_key = Map.get(params, "idempotency_key")
    expected_seq = Map.get(params, "expected_seq")

    with :ok <- require_non_empty_string(producer_id),
         {:ok, producer_seq} <- parse_positive_integer(producer_seq),
         {:ok, actor} <- Policy.resolve_event_actor(auth, Map.get(params, "actor")),
         :ok <- validate_optional_non_empty_string(source),
         {:ok, metadata} <- validate_metadata(metadata),
         {:ok, refs} <- validate_refs(refs),
         :ok <- validate_optional_non_empty_string(idempotency_key),
         {:ok, expected_seq} <- optional_non_neg_integer(expected_seq) do
      {:ok,
       %{
         type: type,
         payload: payload,
         actor: actor,
         source: source,
         metadata: Policy.attach_principal_metadata(auth, metadata),
         refs: refs,
         idempotency_key: idempotency_key,
         producer_id: producer_id,
         producer_seq: producer_seq
       }, expected_seq}
    end
  end

  defp validate(_params, _auth), do: {:error, :invalid_event}

  defp require_non_empty_string(value) when is_binary(value) and value != "", do: :ok
  defp require_non_empty_string(_value), do: {:error, :invalid_event}

  defp parse_positive_integer(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp parse_positive_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> {:ok, parsed}
      _ -> {:error, :invalid_event}
    end
  end

  defp parse_positive_integer(_value), do: {:error, :invalid_event}

  defp validate_optional_non_empty_string(nil), do: :ok
  defp validate_optional_non_empty_string(value) when is_binary(value) and value != "", do: :ok
  defp validate_optional_non_empty_string(_value), do: {:error, :invalid_event}

  defp validate_metadata(%{} = metadata), do: {:ok, metadata}
  defp validate_metadata(_value), do: {:error, :invalid_metadata}

  defp validate_refs(%{} = refs) do
    with {:ok, _to_seq} <- optional_non_neg_integer(refs["to_seq"]),
         :ok <- validate_optional_string(refs["request_id"]),
         :ok <- validate_optional_string(refs["sequence_id"]),
         {:ok, _step} <- optional_non_neg_integer(refs["step"]) do
      {:ok, refs}
    end
  end

  defp validate_refs(_value), do: {:error, :invalid_refs}

  defp validate_optional_string(nil), do: :ok
  defp validate_optional_string(value) when is_binary(value), do: :ok
  defp validate_optional_string(_value), do: {:error, :invalid_event}

  defp optional_non_neg_integer(nil), do: {:ok, nil}
  defp optional_non_neg_integer(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp optional_non_neg_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, :invalid_event}
    end
  end

  defp optional_non_neg_integer(_value), do: {:error, :invalid_event}

  defp measure_append_request(fun) when is_function(fun, 0) do
    started_at = System.monotonic_time()
    result = fun.()
    duration_ms = elapsed_ms_since(started_at)
    :ok = Telemetry.request(:append_event, :total, request_outcome(result), duration_ms)
    result
  end

  defp request_outcome({:ok, _reply}), do: :ok
  defp request_outcome({:timeout, _leader}), do: :timeout
  defp request_outcome(_result), do: :error

  defp elapsed_ms_since(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end
end
