defmodule FleetLMWeb.HealthController do
  use FleetLMWeb, :controller

  alias FleetLM.Runtime.RaftTopology

  @doc """
  GET /health/live

  Liveness probe - returns ok when node is accepting traffic.
  """
  def live(conn, _params) do
    json(conn, %{status: "ok"})
  end

  @doc """
  GET /health/ready

  Readiness probe - returns ok when Raft groups have leaders.
  """
  def ready(conn, _params) do
    if RaftTopology.ready?() do
      json(conn, %{status: "ok"})
    else
      conn
      |> put_status(:service_unavailable)
      |> json(%{status: "starting", reason: "raft_sync"})
    end
  end
end
