defmodule FleetLMWeb.FallbackController do
  @moduledoc """
  Fallback controller for API error handling.
  """
  use FleetLMWeb, :controller

  def call(conn, {:error, :session_not_found}) do
    error(conn, :not_found, "session_not_found", "Session was not found")
  end

  def call(conn, {:error, :session_exists}) do
    error(conn, :conflict, "session_exists", "Session already exists")
  end

  def call(conn, {:error, {:expected_seq_conflict, expected, current}}) do
    error(
      conn,
      :conflict,
      "expected_seq_conflict",
      "Expected seq #{expected}, current seq is #{current}"
    )
  end

  def call(conn, {:error, {:expected_seq_conflict, current}}) do
    error(conn, :conflict, "expected_seq_conflict", "Current seq is #{current}")
  end

  def call(conn, {:error, :idempotency_conflict}) do
    error(
      conn,
      :conflict,
      "idempotency_conflict",
      "Idempotency key was already used with different event content"
    )
  end

  def call(conn, {:timeout, _leader}) do
    error(conn, :service_unavailable, "raft_timeout", "Cluster request timed out")
  end

  def call(conn, {:error, {:timeout, _leader}}) do
    error(conn, :service_unavailable, "raft_timeout", "Cluster request timed out")
  end

  def call(conn, {:error, {:no_available_replicas, _failures}}) do
    error(conn, :service_unavailable, "raft_unavailable", "No available replicas")
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
    error(conn, :bad_request, to_string(reason), reason_message(reason))
  end

  def call(conn, {:error, _reason}) do
    error(conn, :internal_server_error, "internal_error", "Internal server error")
  end

  defp reason_message(:invalid_event), do: "Invalid event payload"
  defp reason_message(:invalid_metadata), do: "Invalid metadata payload"
  defp reason_message(:invalid_refs), do: "Invalid refs payload"
  defp reason_message(:invalid_cursor), do: "Invalid cursor value"
  defp reason_message(:invalid_websocket_upgrade), do: "WebSocket upgrade required"
  defp reason_message(:invalid_session), do: "Invalid session payload"
  defp reason_message(:invalid_session_id), do: "Invalid session id"

  defp error(conn, status, error, message) do
    conn
    |> put_status(status)
    |> json(%{error: error, message: message})
  end
end
