defmodule FastpacaWeb.FallbackController do
  @moduledoc """
  Fallback controller for error handling.

  Error codes per spec:
  - 400 Bad Request: invalid payload shape
  - 404 Not Found: conversation does not exist
  - 409 Conflict: version mismatch
  - 410 Gone: conversation is tombstoned
  """
  use FastpacaWeb, :controller

  def call(conn, {:error, :not_found}) do
    conn
    |> put_status(:not_found)
    |> json(%{error: "not_found"})
  end

  def call(conn, {:error, :conversation_not_found}) do
    conn
    |> put_status(:not_found)
    |> json(%{error: "conversation_not_found"})
  end

  # Legacy support during migration
  def call(conn, {:error, :context_not_found}) do
    conn
    |> put_status(:not_found)
    |> json(%{error: "conversation_not_found"})
  end

  def call(conn, {:error, {:version_conflict, current}}) do
    conn
    |> put_status(:conflict)
    |> json(%{error: "conflict", message: "Version mismatch (current: #{current})"})
  end

  # Tombstoned conversations return 410 Gone per spec
  def call(conn, {:error, :conversation_tombstoned}) do
    conn
    |> put_status(:gone)
    |> json(%{error: "conversation_tombstoned", message: "Conversation is tombstoned"})
  end

  # Legacy support during migration
  def call(conn, {:error, :context_tombstoned}) do
    conn
    |> put_status(:gone)
    |> json(%{error: "conversation_tombstoned", message: "Conversation is tombstoned"})
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
             :invalid_message,
             :invalid_metadata,
             :invalid_conversation_config,
             # Legacy
             :invalid_replacement,
             :invalid_replacement_message,
             :invalid_context_config
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
