defmodule StarciteWeb.SessionController do
  @moduledoc """
  HTTP controller for Starcite session primitives:

  - `create` via `POST /v1/sessions`
  - `append` via `POST /v1/sessions/:id/append`
  """

  use StarciteWeb, :controller

  alias Starcite.Runtime

  plug OpenApiSpex.Plug.CastAndValidate,
       [operation_id: "SessionCreate", json_render_error_v2: true]
       when action in [:create]

  plug OpenApiSpex.Plug.CastAndValidate,
       [operation_id: "SessionAppend", json_render_error_v2: true]
       when action in [:append]

  action_fallback StarciteWeb.FallbackController

  @doc """
  Create a session.
  """
  def create(conn, _params) do
    opts = build_create_opts(conn.body_params)

    with {:ok, session} <- Runtime.create_session(opts) do
      conn
      |> put_status(:created)
      |> json(session)
    end
  end

  @doc """
  Append one event to a session.
  """
  def append(conn, params) do
    with {:ok, id} <- extract_session_id(params),
         {:ok, event, expected_seq} <- build_append(conn.body_params),
         {:ok, reply} <- Runtime.append_event(id, event, expected_seq: expected_seq) do
      conn
      |> put_status(:created)
      |> json(reply)
    end
  end

  defp build_create_opts(params) when is_map(params) do
    [
      id: get_param(params, :id),
      title: get_param(params, :title),
      metadata: get_param(params, :metadata, %{})
    ]
  end

  defp build_create_opts(_params), do: [id: nil, title: nil, metadata: %{}]

  defp extract_session_id(params) when is_map(params) do
    case get_param(params, :id) do
      id when is_binary(id) and id != "" -> {:ok, id}
      _ -> {:error, :invalid_session_id}
    end
  end

  defp extract_session_id(_params), do: {:error, :invalid_session_id}

  defp build_append(params) when is_map(params) do
    event = %{
      type: get_param(params, :type),
      payload: get_param(params, :payload, %{}),
      actor: get_param(params, :actor),
      source: get_param(params, :source),
      metadata: get_param(params, :metadata, %{}),
      refs: get_param(params, :refs, %{}),
      idempotency_key: get_param(params, :idempotency_key)
    }

    {:ok, event, get_param(params, :expected_seq)}
  end

  defp build_append(_params), do: {:error, :invalid_event}

  defp get_param(params, key, default \\ nil) do
    case Map.fetch(params, key) do
      {:ok, value} -> value
      :error -> Map.get(params, Atom.to_string(key), default)
    end
  end
end
