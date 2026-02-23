defmodule StarciteWeb.HealthControllerTest do
  use ExUnit.Case, async: false

  import Plug.Test

  alias Starcite.ControlPlane.{Observer, Ops}

  @endpoint StarciteWeb.Endpoint

  setup do
    Starcite.Runtime.TestHelper.reset()
    :ok
  end

  defp request(path) when is_binary(path) do
    conn(:get, path)
    |> @endpoint.call(@endpoint.init([]))
  end

  describe "GET /health/live" do
    test "returns ok" do
      conn = request("/health/live")

      assert conn.status == 200
      assert Jason.decode!(conn.resp_body) == %{"status" => "ok"}
    end
  end

  describe "GET /health/ready" do
    test "returns readiness status with role mode" do
      conn = request("/health/ready")
      body = Jason.decode!(conn.resp_body)

      assert body["mode"] in ["write_node", "router_node"]
      assert body["status"] in ["ok", "starting"]

      if conn.status == 200 do
        assert body["status"] == "ok"
      else
        assert conn.status == 503
        assert body["status"] == "starting"
        assert body["reason"] in ["raft_sync", "router_sync", "draining"]
      end
    end

    test "reports raft_sync when write node is undrained but not raft-ready" do
      assert Ops.local_mode() == :write_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_state = :sys.get_state(Observer)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_state end)
      end)

      :sys.replace_state(Observer, fn %{raft_ready_nodes: raft_ready_nodes} = state ->
        %{state | raft_ready_nodes: MapSet.delete(raft_ready_nodes, local)}
      end)

      conn = request("/health/ready")
      body = Jason.decode!(conn.resp_body)

      assert conn.status == 503
      assert body["status"] == "starting"
      assert body["mode"] == "write_node"
      assert body["reason"] == "raft_sync"
    end
  end
end
