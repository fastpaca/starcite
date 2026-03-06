defmodule StarciteWeb.FallbackController do
  @moduledoc """
  Fallback controller for API error handling.
  """
  use StarciteWeb, :controller

  def call(conn, {:timeout, _leader} = error), do: respond(conn, error)
  def call(conn, {:error, _reason} = error), do: respond(conn, error)
  def call(conn, _error), do: respond(conn, {:error, :internal_error})

  @spec error_details({:error, term()} | {:timeout, term()}) ::
          {Plug.Conn.status(), String.t(), String.t()}
  def error_details({:error, :session_not_found}),
    do: {:not_found, "session_not_found", "Session was not found"}

  def error_details({:error, :session_exists}),
    do: {:conflict, "session_exists", "Session already exists"}

  def error_details({:error, {:expected_seq_conflict, expected, current}}),
    do:
      {:conflict, "expected_seq_conflict", "Expected seq #{expected}, current seq is #{current}"}

  def error_details({:error, {:expected_seq_conflict, current}}),
    do: {:conflict, "expected_seq_conflict", "Current seq is #{current}"}

  def error_details({:error, {:producer_seq_conflict, producer_id, expected, current}}),
    do:
      {:conflict, "producer_seq_conflict",
       "Producer #{producer_id} expected seq #{expected}, got #{current}"}

  def error_details({:error, :producer_replay_conflict}) do
    {:conflict, "producer_replay_conflict",
     "Producer sequence was already used with different event content"}
  end

  def error_details({:timeout, _leader}),
    do: {:service_unavailable, "raft_timeout", "Cluster request timed out"}

  def error_details({:error, {:timeout, _leader}}),
    do: {:service_unavailable, "raft_timeout", "Cluster request timed out"}

  def error_details({:error, {:no_available_replicas, _failures}}),
    do: {:service_unavailable, "raft_unavailable", "No available replicas"}

  def error_details({:error, :archive_read_unavailable}),
    do: {:service_unavailable, "archive_read_unavailable", "Archive read unavailable"}

  def error_details({:error, :event_gap_detected}),
    do: {:service_unavailable, "event_gap_detected", "Ordered replay gap detected"}

  def error_details({:error, :event_store_backpressure}) do
    {:too_many_requests, "event_store_backpressure",
     "Node is backpressuring writes due to event-store capacity"}
  end

  def error_details({:error, :forbidden}), do: {:forbidden, "forbidden", "Forbidden"}
  def error_details({:error, :unauthorized}), do: {:unauthorized, "unauthorized", "Unauthorized"}

  def error_details({:error, :forbidden_scope}) do
    {:forbidden, "forbidden_scope", "Token scope does not allow this operation"}
  end

  def error_details({:error, :forbidden_session}) do
    {:forbidden, "forbidden_session", "Token is not allowed to access this session"}
  end

  def error_details({:error, :forbidden_tenant}) do
    {:forbidden, "forbidden_tenant", "Token tenant does not match session tenant"}
  end

  def error_details({:error, reason})
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
           ] do
    {:bad_request, to_string(reason), reason_message(reason)}
  end

  def error_details({:error, _reason}),
    do: {:internal_server_error, "internal_error", "Internal server error"}

  defp respond(conn, error) do
    {status, code, message} = error_details(error)
    error(conn, status, code, message)
  end

  defp reason_message(:invalid_event), do: "Invalid event payload"
  defp reason_message(:invalid_metadata), do: "Invalid metadata payload"
  defp reason_message(:invalid_refs), do: "Invalid refs payload"
  defp reason_message(:invalid_cursor), do: "Invalid cursor value"
  defp reason_message(:invalid_tail_batch_size), do: "Invalid tail batch size value"
  defp reason_message(:invalid_limit), do: "Invalid limit value"
  defp reason_message(:invalid_list_query), do: "Invalid list query"
  defp reason_message(:invalid_write_node), do: "Invalid write node"
  defp reason_message(:invalid_group_id), do: "Invalid write group id"
  defp reason_message(:invalid_websocket_upgrade), do: "WebSocket upgrade required"
  defp reason_message(:invalid_session), do: "Invalid session payload"
  defp reason_message(:invalid_session_id), do: "Invalid session id"

  defp error(conn, status, error, message) do
    conn
    |> put_status(status)
    |> json(%{error: error, message: message})
  end
end
