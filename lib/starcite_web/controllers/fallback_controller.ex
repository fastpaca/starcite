defmodule StarciteWeb.FallbackController do
  @moduledoc """
  Fallback controller for API error handling.
  """
  use StarciteWeb, :controller

  alias StarciteWeb.ErrorInfo

  def call(conn, result) do
    %{status: status, error: error, message: message} = ErrorInfo.from_result(result)

    conn
    |> put_status(status)
    |> json(%{error: error, message: message})
  end
end
