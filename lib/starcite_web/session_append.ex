defmodule StarciteWeb.SessionAppend do
  @moduledoc false

  alias Starcite.Observability.Telemetry
  alias Starcite.WritePath
  alias StarciteWeb.Auth.{Context, Policy}
  alias StarciteWeb.ErrorInfo
  alias StarciteWeb.TailAccess

  @spec append(Context.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def append(%Context{} = auth, session_id, params)
      when is_binary(session_id) and session_id != "" and is_map(params) do
    measure_append_request(fn ->
      with {:ok, _session} <- TailAccess.authorize_append(auth, session_id),
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

  defp append_event(session_id, event, nil), do: WritePath.append_event(session_id, event)

  defp append_event(session_id, event, expected_seq) do
    WritePath.append_event(session_id, event, expected_seq: expected_seq)
  end

  defp validate(
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
          {:ok,
           %{
             type: type,
             payload: payload,
             actor: actor,
             source: source,
             metadata: Policy.attach_principal_metadata(auth, metadata),
             refs: %{},
             idempotency_key: nil,
             producer_id: producer_id,
             producer_seq: producer_seq
           }, nil}
        end

      _other ->
        validate_slow(params, auth)
    end
  end

  defp validate(%{"type" => type, "payload" => payload} = params, %Context{} = auth)
       when is_binary(type) and type != "" and is_map(payload) do
    validate_slow(params, auth)
  end

  defp validate(_params, _auth), do: {:error, :invalid_event}

  defp validate_slow(
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
      {:ok,
       %{
         type: type,
         payload: payload,
         actor: actor,
         source: source,
         metadata: Policy.attach_principal_metadata(auth, metadata),
         refs: refs,
         idempotency_key: idempotency_key,
         producer_id: validated_producer_id,
         producer_seq: validated_producer_seq
       }, expected_seq}
    end
  end

  defp validate_slow(_params, _auth), do: {:error, :invalid_event}

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
    with {:ok, _to_seq} <- optional_non_neg_integer(refs["to_seq"]),
         {:ok, _request_id} <- optional_string(refs["request_id"]),
         {:ok, _sequence_id} <- optional_string(refs["sequence_id"]),
         {:ok, _step} <- optional_non_neg_integer(refs["step"]) do
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
