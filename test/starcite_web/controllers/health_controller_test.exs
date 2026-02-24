defmodule StarciteWeb.HealthControllerTest do
  use ExUnit.Case, async: false

  import Plug.Test

  alias Starcite.ControlPlane.{Observer, Ops}
  alias Starcite.ControlPlane.ObserverState
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
        assert body["reason"] in ["raft_sync", "router_sync", "draining", "observer_sync"]
      end
    end

    test "reports raft_sync when observer is ready but raft convergence is not ready" do
      assert Ops.local_mode() == :write_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_observer_state = :sys.get_state(Observer)
      original_bootstrap_state = :sys.get_state(RaftBootstrap)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_observer_state end)
        :sys.replace_state(RaftBootstrap, fn _state -> original_bootstrap_state end)
      end)

      :sys.replace_state(Observer, fn %{observer: observer, raft_ready_nodes: raft_ready_nodes} =
                                        state ->
        now_ms = System.monotonic_time(:millisecond)
        next_observer = ObserverState.mark_ready(observer, local, now_ms)

        %{
          state
          | observer: next_observer,
            raft_ready_nodes: MapSet.put(raft_ready_nodes, local)
        }
      end)

      :sys.replace_state(RaftBootstrap, fn state ->
        now_ms = System.monotonic_time(:millisecond)

        state
        |> Map.put(:startup_complete?, true)
        |> Map.put(:startup_mode, :follower)
        |> Map.put(:consensus_ready?, false)
        |> Map.put(:consensus_last_probe_at_ms, now_ms)
        |> Map.put(:consensus_probe_success_streak, 0)
        |> Map.put(:consensus_probe_failure_streak, 1)
        |> Map.put(:consensus_probe_detail, %{
          checked_groups: 1,
          failing_group_id: 0,
          probe_result: "timeout"
        })
      end)

      conn = request("/health/ready")
      body = Jason.decode!(conn.resp_body)

      assert conn.status == 503
      assert body["status"] == "starting"
      assert body["mode"] == "write_node"
      assert body["reason"] == "raft_sync"
      assert body["detail"]["probe_result"] == "timeout"
    end

    test "returns ok again after raft convergence recovers" do
      assert Ops.local_mode() == :write_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_observer_state = :sys.get_state(Observer)
      original_bootstrap_state = :sys.get_state(RaftBootstrap)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_observer_state end)
        :sys.replace_state(RaftBootstrap, fn _state -> original_bootstrap_state end)
      end)

      :sys.replace_state(Observer, fn %{observer: observer, raft_ready_nodes: raft_ready_nodes} =
                                        state ->
        now_ms = System.monotonic_time(:millisecond)
        next_observer = ObserverState.mark_ready(observer, local, now_ms)

        %{
          state
          | observer: next_observer,
            raft_ready_nodes: MapSet.put(raft_ready_nodes, local)
        }
      end)

      :sys.replace_state(RaftBootstrap, fn state ->
        now_ms = System.monotonic_time(:millisecond)

        state
        |> Map.put(:startup_complete?, true)
        |> Map.put(:startup_mode, :follower)
        |> Map.put(:consensus_ready?, false)
        |> Map.put(:consensus_last_probe_at_ms, now_ms)
        |> Map.put(:consensus_probe_success_streak, 0)
        |> Map.put(:consensus_probe_failure_streak, 1)
        |> Map.put(:consensus_probe_detail, %{
          checked_groups: 1,
          failing_group_id: 0,
          probe_result: "timeout"
        })
      end)

      conn = request("/health/ready")
      body = Jason.decode!(conn.resp_body)
      assert conn.status == 503
      assert body["reason"] == "raft_sync"

      :sys.replace_state(RaftBootstrap, fn state ->
        now_ms = System.monotonic_time(:millisecond)

        state
        |> Map.put(:consensus_ready?, true)
        |> Map.put(:consensus_last_probe_at_ms, now_ms)
        |> Map.put(:consensus_probe_success_streak, 1)
        |> Map.put(:consensus_probe_failure_streak, 0)
        |> Map.put(:consensus_probe_detail, %{
          checked_groups: 1,
          failing_group_id: nil,
          probe_result: "ok"
        })
      end)

      conn = request("/health/ready")
      body = Jason.decode!(conn.resp_body)
      assert conn.status == 200
      assert body["status"] == "ok"
      assert body["mode"] == "write_node"
    end

    test "reports observer_sync when raft is healthy but observer excludes write node" do
      assert Ops.local_mode() == :write_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_observer_state = :sys.get_state(Observer)
      original_bootstrap_state = :sys.get_state(RaftBootstrap)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_observer_state end)
        :sys.replace_state(RaftBootstrap, fn _state -> original_bootstrap_state end)
      end)

      :sys.replace_state(Observer, fn %{raft_ready_nodes: raft_ready_nodes} = state ->
        %{state | raft_ready_nodes: MapSet.delete(raft_ready_nodes, local)}
      end)

      :sys.replace_state(RaftBootstrap, fn state ->
        now_ms = System.monotonic_time(:millisecond)

        state
        |> Map.put(:startup_complete?, true)
        |> Map.put(:startup_mode, :follower)
        |> Map.put(:consensus_ready?, true)
        |> Map.put(:consensus_last_probe_at_ms, now_ms)
        |> Map.put(:consensus_probe_success_streak, 1)
        |> Map.put(:consensus_probe_failure_streak, 0)
        |> Map.put(:consensus_probe_detail, %{
          checked_groups: 1,
          failing_group_id: nil,
          probe_result: "ok"
        })
      end)

      conn = request("/health/ready")
      body = Jason.decode!(conn.resp_body)

      assert conn.status == 503
      assert body["status"] == "starting"
      assert body["mode"] == "write_node"
      assert body["reason"] == "observer_sync"
    end
  end
end
