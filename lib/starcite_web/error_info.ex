defmodule StarciteWeb.ErrorInfo do
  @moduledoc false

  @type t :: %{
          status: atom(),
          error: String.t(),
          message: String.t()
        }

  @invalid_request_reasons [
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
  ]

  @spec from_result(term()) :: t()
  def from_result({:error, :session_not_found}) do
    %{status: :not_found, error: "session_not_found", message: "Session was not found"}
  end

  def from_result({:error, :session_exists}) do
    %{status: :conflict, error: "session_exists", message: "Session already exists"}
  end

  def from_result({:error, {:expected_seq_conflict, expected, current}}) do
    %{
      status: :conflict,
      error: "expected_seq_conflict",
      message: "Expected seq #{expected}, current seq is #{current}"
    }
  end

  def from_result({:error, {:expected_seq_conflict, current}}) do
    %{
      status: :conflict,
      error: "expected_seq_conflict",
      message: "Current seq is #{current}"
    }
  end

  def from_result({:error, {:producer_seq_conflict, producer_id, expected, current}}) do
    %{
      status: :conflict,
      error: "producer_seq_conflict",
      message: "Producer #{producer_id} expected seq #{expected}, got #{current}"
    }
  end

  def from_result({:error, :producer_replay_conflict}) do
    %{
      status: :conflict,
      error: "producer_replay_conflict",
      message: "Producer sequence was already used with different event content"
    }
  end

  def from_result({:timeout, _leader}) do
    %{status: :service_unavailable, error: "raft_timeout", message: "Cluster request timed out"}
  end

  def from_result({:error, {:timeout, _leader}}) do
    %{status: :service_unavailable, error: "raft_timeout", message: "Cluster request timed out"}
  end

  def from_result({:error, {:no_available_replicas, _failures}}) do
    %{status: :service_unavailable, error: "raft_unavailable", message: "No available replicas"}
  end

  def from_result({:error, :archive_read_unavailable}) do
    %{
      status: :service_unavailable,
      error: "archive_read_unavailable",
      message: "Archive read unavailable"
    }
  end

  def from_result({:error, :event_gap_detected}) do
    %{
      status: :service_unavailable,
      error: "event_gap_detected",
      message: "Ordered replay gap detected"
    }
  end

  def from_result({:error, :event_store_backpressure}) do
    %{
      status: :too_many_requests,
      error: "event_store_backpressure",
      message: "Node is backpressuring writes due to event-store capacity"
    }
  end

  def from_result({:error, :forbidden}) do
    %{status: :forbidden, error: "forbidden", message: "Forbidden"}
  end

  def from_result({:error, :unauthorized}) do
    %{status: :unauthorized, error: "unauthorized", message: "Unauthorized"}
  end

  def from_result({:error, :forbidden_scope}) do
    %{
      status: :forbidden,
      error: "forbidden_scope",
      message: "Token scope does not allow this operation"
    }
  end

  def from_result({:error, :forbidden_session}) do
    %{
      status: :forbidden,
      error: "forbidden_session",
      message: "Token is not allowed to access this session"
    }
  end

  def from_result({:error, :forbidden_tenant}) do
    %{
      status: :forbidden,
      error: "forbidden_tenant",
      message: "Token tenant does not match session tenant"
    }
  end

  def from_result({:error, reason}) when reason in @invalid_request_reasons do
    %{status: :bad_request, error: to_string(reason), message: reason_message(reason)}
  end

  def from_result({:error, _reason}) do
    %{status: :internal_server_error, error: "internal_error", message: "Internal server error"}
  end

  @spec payload(term()) :: %{error: String.t(), message: String.t()}
  def payload(result) do
    %{error: error, message: message} = from_result(result)
    %{error: error, message: message}
  end

  @spec ingest_edge_reason(term()) :: atom()
  def ingest_edge_reason({:error, {:not_leader, _leader}}), do: :not_leader
  def ingest_edge_reason({:error, :not_leader}), do: :not_leader

  def ingest_edge_reason({:error, {:expected_seq_conflict, _expected, _current}}),
    do: :seq_conflict

  def ingest_edge_reason({:error, {:expected_seq_conflict, _current}}), do: :seq_conflict

  def ingest_edge_reason({:error, {:producer_seq_conflict, _producer_id, _expected, _got}}),
    do: :seq_conflict

  def ingest_edge_reason({:error, :producer_replay_conflict}), do: :seq_conflict
  def ingest_edge_reason({:timeout, _leader}), do: :timeout
  def ingest_edge_reason({:error, {:timeout, _leader}}), do: :timeout
  def ingest_edge_reason({:error, {:no_available_replicas, _failures}}), do: :unavailable

  def ingest_edge_reason({:error, reason})
      when reason in [:archive_read_unavailable, :event_gap_detected, :event_store_backpressure],
      do: :unavailable

  def ingest_edge_reason({:error, :unauthorized}), do: :unauthorized

  def ingest_edge_reason({:error, reason})
      when reason in [:forbidden, :forbidden_scope, :forbidden_session, :forbidden_tenant],
      do: :forbidden

  def ingest_edge_reason({:error, :session_not_found}), do: :session_not_found
  def ingest_edge_reason({:error, :session_exists}), do: :session_exists

  def ingest_edge_reason({:error, reason}) when reason in @invalid_request_reasons,
    do: :invalid_request

  def ingest_edge_reason({:error, reason}) when is_atom(reason), do: reason

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
end
