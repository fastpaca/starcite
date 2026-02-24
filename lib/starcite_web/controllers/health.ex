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
    readiness = Ops.local_readiness(refresh?: true)

    if readiness.ready? do
      json(conn, %{status: "ok", mode: mode})
    else
      body = %{
        status: "starting",
        mode: mode,
        reason: readiness_reason(readiness.reason, mode)
      }

      conn
      |> put_status(:service_unavailable)
      |> json(put_detail(body, readiness.detail))
    end
  end

  defp readiness_reason(:draining, _mode), do: "draining"
  defp readiness_reason(:observer_sync, "write_node"), do: "observer_sync"
  defp readiness_reason(:startup_sync, "write_node"), do: "raft_sync"
  defp readiness_reason(:raft_sync, "write_node"), do: "raft_sync"
  defp readiness_reason(:bootstrap_down, "write_node"), do: "raft_sync"
  defp readiness_reason(_reason, "write_node"), do: "raft_sync"

  defp readiness_reason(:startup_sync, _mode), do: "router_sync"
  defp readiness_reason(:bootstrap_down, _mode), do: "router_sync"
  defp readiness_reason(_reason, _mode), do: "router_sync"

  defp put_detail(body, detail) when map_size(detail) == 0, do: body
  defp put_detail(body, detail), do: Map.put(body, :detail, detail)
end
