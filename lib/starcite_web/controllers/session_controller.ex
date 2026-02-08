defmodule StarciteWeb.SessionController do
  @moduledoc """
  HTTP controller for Starcite session primitives:

  - `create` via `POST /v1/sessions`
  - `append` via `POST /v1/sessions/:id/append`
  """

  use StarciteWeb, :controller

  alias Starcite.Runtime

  action_fallback StarciteWeb.FallbackController

  @doc """
  Create a session.
  """
  def create(conn, params) do
    with {:ok, opts} <- validate_create(params),
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
    with {:ok, event, expected_seq} <- validate_append(params),
         {:ok, reply} <- Runtime.append_event(id, event, expected_seq: expected_seq) do
      conn
      |> put_status(:created)
      |> json(reply)
    end
  end

  def append(_conn, _params), do: {:error, :invalid_event}

  # ---------------------------------------------------------------------------
  # Validation
  # ---------------------------------------------------------------------------

  defp validate_create(params) when is_map(params) do
    with {:ok, id} <- optional_non_empty_string(Map.get(params, "id")),
         {:ok, title} <- optional_string(Map.get(params, "title")),
         {:ok, metadata} <- optional_object(Map.get(params, "metadata")) do
      {:ok, [id: id, title: title, metadata: metadata]}
    end
  end

  defp validate_append(%{"type" => type, "payload" => payload, "actor" => actor} = params)
       when is_binary(type) and type != "" and is_map(payload) and is_binary(actor) and
              actor != "" do
    with {:ok, source} <- optional_non_empty_string(Map.get(params, "source")),
         {:ok, metadata} <- optional_object(Map.get(params, "metadata")),
         {:ok, refs} <- optional_refs(Map.get(params, "refs")),
         {:ok, idempotency_key} <- optional_non_empty_string(Map.get(params, "idempotency_key")),
         {:ok, expected_seq} <- optional_non_neg_integer(Map.get(params, "expected_seq")) do
      event = %{
        type: type,
        payload: payload,
        actor: actor,
        source: source,
        metadata: metadata,
        refs: refs,
        idempotency_key: idempotency_key
      }

      {:ok, event, expected_seq}
    end
  end

  defp validate_append(_params), do: {:error, :invalid_event}

  defp optional_non_empty_string(nil), do: {:ok, nil}
  defp optional_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp optional_non_empty_string(_value), do: {:error, :invalid_event}

  defp optional_object(nil), do: {:ok, %{}}
  defp optional_object(value) when is_map(value) and not is_list(value), do: {:ok, value}
  defp optional_object(_value), do: {:error, :invalid_metadata}

  defp optional_refs(nil), do: {:ok, %{}}

  defp optional_refs(refs) when is_map(refs) and not is_list(refs) do
    with {:ok, _} <- optional_non_neg_integer(Map.get(refs, "to_seq")),
         {:ok, _} <- optional_string(Map.get(refs, "request_id")),
         {:ok, _} <- optional_string(Map.get(refs, "sequence_id")),
         {:ok, _} <- optional_non_neg_integer(Map.get(refs, "step")) do
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
