defmodule StarciteWeb.FallbackController do
  @moduledoc """
  Fallback controller for API error handling.
  """
  use StarciteWeb, :controller

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

  def call(conn, {:error, {:producer_seq_conflict, producer_id, expected, current}}) do
    error(
      conn,
      :conflict,
      "producer_seq_conflict",
      "Producer #{producer_id} expected seq #{expected}, got #{current}"
    )
  end

  def call(conn, {:error, :producer_replay_conflict}) do
    error(
      conn,
      :conflict,
      "producer_replay_conflict",
      "Producer sequence was already used with different event content"
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

  def call(conn, {:error, :archive_read_unavailable}) do
    error(conn, :service_unavailable, "archive_read_unavailable", "Archive read unavailable")
  end

  def call(conn, {:error, :event_gap_detected}) do
    error(conn, :service_unavailable, "event_gap_detected", "Ordered replay gap detected")
  end

  def call(conn, {:error, :event_store_backpressure}) do
    error(
      conn,
      :too_many_requests,
      "event_store_backpressure",
      "Node is backpressuring writes due to event-store capacity"
    )
  end

  def call(conn, {:error, :forbidden}) do
    error(conn, :forbidden, "forbidden", "Forbidden")
  end

  def call(conn, {:error, :forbidden_scope}) do
    error(conn, :forbidden, "forbidden_scope", "Token scope does not allow this operation")
  end

  def call(conn, {:error, :forbidden_session}) do
    error(conn, :forbidden, "forbidden_session", "Token is not allowed to access this session")
  end

  def call(conn, {:error, :forbidden_tenant}) do
    error(conn, :forbidden, "forbidden_tenant", "Token tenant does not match session tenant")
  end

  def call(conn, {:error, reason})
      when reason in [
             :invalid_event,
             :invalid_metadata,
             :invalid_refs,
             :invalid_cursor,
             :invalid_limit,
             :invalid_list_query,
             :invalid_issue_request,
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
  defp reason_message(:invalid_limit), do: "Invalid limit value"
  defp reason_message(:invalid_list_query), do: "Invalid list query"
  defp reason_message(:invalid_issue_request), do: "Invalid token issue request payload"
  defp reason_message(:invalid_websocket_upgrade), do: "WebSocket upgrade required"
  defp reason_message(:invalid_session), do: "Invalid session payload"
  defp reason_message(:invalid_session_id), do: "Invalid session id"

  defp error(conn, status, error, message) do
    conn
    |> put_status(status)
    |> json(%{error: error, message: message})
  end
end
