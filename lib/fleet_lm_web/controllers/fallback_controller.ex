defmodule FleetLMWeb.FallbackController do
  @moduledoc """
  Fallback controller for API error handling.
  """
  use FleetLMWeb, :controller

  def call(conn, {:error, :session_not_found}) do
    conn
    |> put_status(:not_found)
    |> json(%{error: "session_not_found"})
  end

  def call(conn, {:error, :session_exists}) do
    conn
    |> put_status(:conflict)
    |> json(%{error: "session_exists"})
  end

  def call(conn, {:error, {:expected_seq_conflict, current}}) do
    conn
    |> put_status(:conflict)
    |> json(%{
      error: "expected_seq_conflict",
      message: "Expected seq mismatch (current: #{current})"
    })
  end

  def call(conn, {:error, :idempotency_conflict}) do
    conn
    |> put_status(:conflict)
    |> json(%{error: "idempotency_conflict"})
  end

  def call(conn, {:timeout, _leader}) do
    conn
    |> put_status(:service_unavailable)
    |> json(%{error: "raft_timeout"})
  end

  def call(conn, {:error, {:no_available_replicas, _failures}}) do
    conn
    |> put_status(:service_unavailable)
    |> json(%{error: "raft_unavailable"})
  end

  def call(conn, {:error, reason})
      when reason in [
             :invalid_event,
             :invalid_metadata,
             :invalid_refs,
             :invalid_cursor,
             :invalid_websocket_upgrade,
             :invalid_session,
             :invalid_session_id
           ] do
    conn
    |> put_status(:bad_request)
    |> json(%{error: to_string(reason)})
  end

  def call(conn, {:error, reason}) do
    conn
    |> put_status(:internal_server_error)
    |> json(%{error: inspect(reason)})
  end
end
