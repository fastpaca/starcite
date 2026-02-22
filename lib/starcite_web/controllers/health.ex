defmodule StarciteWeb.HealthController do
  use StarciteWeb, :controller

  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.WritePath.RaftTopology

  @doc """
  GET /health/live

  Liveness probe - returns ok when node is accepting traffic.
  """
  def live(conn, _params) do
    json(conn, %{status: "ok"})
  end

  @doc """
  GET /health/ready

  Readiness probe - returns ok when local node role startup is complete.
  """
  def ready(conn, _params) do
    mode = readiness_mode()

    if RaftTopology.ready?() do
      json(conn, %{status: "ok", mode: mode})
    else
      reason =
        case mode do
          "write_node" -> "raft_sync"
          _ -> "router_sync"
        end

      conn
      |> put_status(:service_unavailable)
      |> json(%{status: "starting", mode: mode, reason: reason})
    end
  end

  defp readiness_mode do
    if WriteNodes.write_node?(Node.self()) do
      "write_node"
    else
      "router_node"
    end
  end
end
