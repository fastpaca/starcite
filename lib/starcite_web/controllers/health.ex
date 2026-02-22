defmodule StarciteWeb.HealthController do
  use StarciteWeb, :controller

  alias Starcite.ControlPlane.Ops

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
    mode = Ops.local_mode() |> Atom.to_string()

    if Ops.local_ready() do
      json(conn, %{status: "ok", mode: mode})
    else
      conn
      |> put_status(:service_unavailable)
      |> json(%{status: "starting", mode: mode, reason: readiness_reason(mode)})
    end
  end

  defp readiness_reason("write_node"),
    do: if(Ops.local_drained(), do: "draining", else: "raft_sync")

  defp readiness_reason(_mode), do: "router_sync"
end
