defmodule StarciteWeb.HealthControllerTest do
  use ExUnit.Case, async: false

  import Plug.Test

  alias Starcite.ControlPlane.{Observer, Ops}
  alias Starcite.DataPlane.RaftBootstrap

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

    test "returns ok once follower bootstrap is complete" do
      assert Ops.local_mode() == :write_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_observer_state = :sys.get_state(Observer)
      original_bootstrap_state = :sys.get_state(RaftBootstrap)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_observer_state end)
        :sys.replace_state(RaftBootstrap, fn _state -> original_bootstrap_state end)
      end)

      :sys.replace_state(RaftBootstrap, fn state ->
        state
        |> Map.put(:startup_complete?, true)
        |> Map.put(:startup_mode, :follower)
        |> Map.put(:consensus_ready?, false)
      end)

      :sys.replace_state(Observer, fn %{raft_ready_nodes: raft_ready_nodes} = state ->
        %{state | raft_ready_nodes: MapSet.delete(raft_ready_nodes, local)}
      end)

      send(Observer, :maintenance)

      eventually(fn ->
        conn = request("/health/ready")
        body = Jason.decode!(conn.resp_body)

        assert conn.status == 200
        assert body["status"] == "ok"
        assert body["mode"] == "write_node"
      end)
    end
  end

  defp eventually(fun, opts \\ []) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 1_000)
    interval = Keyword.get(opts, :interval, 25)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    try do
      fun.()
    rescue
      error in [ExUnit.AssertionError] ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        else
          reraise(error, __STACKTRACE__)
        end
    end
  end
end
