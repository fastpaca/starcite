defmodule StarciteWeb.HealthController do
  use StarciteWeb, :controller

  alias StarciteWeb.Health

  @doc """
  GET /health/live

  Liveness probe - returns ok when node is accepting traffic.
  """
  def live(conn, _params) do
    {_status, body} = Health.live()
    json(conn, body)
  end

  @doc """
  GET /health/ready

  Readiness probe - returns ok when local node role startup is complete.
  """
  def ready(conn, _params) do
    {status, body} = Health.ready()

    conn
    |> put_status(status)
    |> json(body)
  end
end
