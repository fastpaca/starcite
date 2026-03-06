defmodule StarciteWeb.SessionAppend do
  @moduledoc false

  alias Starcite.Session
  alias Starcite.WritePath
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.Policy

  @type append_reply :: %{
          seq: non_neg_integer(),
          last_seq: non_neg_integer(),
          deduped: boolean()
        }

  @spec append(String.t(), map(), Context.t(), Session.t()) ::
          {:ok, append_reply()} | {:error, term()} | {:timeout, term()}
  def append(session_id, params, %Context{} = auth, %Session{id: session_id} = session)
      when is_binary(session_id) and session_id != "" and is_map(params) do
    with :ok <- Policy.allowed_to_append_session(auth, session),
         {:ok, event, expected_seq} <- validate(params, auth) do
      append_event(session_id, event, expected_seq)
    end
  end

  def append(_session_id, _params, _auth, _session), do: {:error, :invalid_event}

  @spec validate(map(), Context.t()) :: {:ok, map(), non_neg_integer() | nil} | {:error, atom()}
  def validate(
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
        validate_slow(params, auth)
    end
  end

  def validate(
        %{
          "type" => type,
          "payload" => payload
        } = params,
        %Context{} = auth
      )
      when is_binary(type) and type != "" and is_map(payload) do
    validate_slow(params, auth)
  end

  def validate(_params, _auth), do: {:error, :invalid_event}

  defp append_event(session_id, event, nil), do: WritePath.append_event(session_id, event)

  defp append_event(session_id, event, expected_seq),
    do: WritePath.append_event(session_id, event, expected_seq: expected_seq)

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
end
