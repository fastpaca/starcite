defmodule StarciteWeb.OpsRouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.Operations, as: Ops
  alias Starcite.Routing.Observer
  alias Starcite.Routing.ObserverState
  alias Starcite.Routing.LeaseBootstrap

  @endpoint StarciteWeb.Endpoint
  @ops_router StarciteWeb.OpsRouter

  setup do
    Starcite.Runtime.TestHelper.reset()
    :ok
  end

  defp request(path) when is_binary(path) do
    conn(:get, path)
    |> @ops_router.call(@ops_router.init([]))
  end

  defp public_request(path) when is_binary(path) do
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

      assert body["mode"] in ["routing_node", "ingress_node"]
      assert body["status"] in ["ok", "starting"]

      if conn.status == 200 do
        assert body["status"] == "ok"
      else
        assert conn.status == 503
        assert body["status"] == "starting"
        assert body["reason"] in ["lease_sync", "routing_sync", "draining", "observer_sync"]
      end
    end

    test "reports lease_sync when observer is ready but raft convergence is not ready" do
      assert Ops.local_mode() == :routing_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_observer_state = :sys.get_state(Observer)
      original_bootstrap_state = :sys.get_state(LeaseBootstrap)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_observer_state end)
        :sys.replace_state(LeaseBootstrap, fn _state -> original_bootstrap_state end)
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

      :sys.replace_state(LeaseBootstrap, fn state ->
        now_ms = System.monotonic_time(:millisecond)

        state
        |> Map.put(:startup_complete?, true)
        |> Map.put(:startup_mode, :routing)
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
      assert body["mode"] == "routing_node"
      assert body["reason"] == "lease_sync"
      assert body["detail"]["probe_result"] == "timeout"
    end

    test "returns ok again after raft convergence recovers" do
      assert Ops.local_mode() == :routing_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_observer_state = :sys.get_state(Observer)
      original_bootstrap_state = :sys.get_state(LeaseBootstrap)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_observer_state end)
        :sys.replace_state(LeaseBootstrap, fn _state -> original_bootstrap_state end)
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

      :sys.replace_state(LeaseBootstrap, fn state ->
        now_ms = System.monotonic_time(:millisecond)

        state
        |> Map.put(:startup_complete?, true)
        |> Map.put(:startup_mode, :routing)
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
      assert body["reason"] == "lease_sync"

      :sys.replace_state(LeaseBootstrap, fn state ->
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
      assert body["mode"] == "routing_node"
    end

    test "reports observer_sync when raft is healthy but observer excludes routing node" do
      assert Ops.local_mode() == :routing_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_observer_state = :sys.get_state(Observer)
      original_bootstrap_state = :sys.get_state(LeaseBootstrap)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_observer_state end)
        :sys.replace_state(LeaseBootstrap, fn _state -> original_bootstrap_state end)
      end)

      :sys.replace_state(Observer, fn %{raft_ready_nodes: raft_ready_nodes} = state ->
        %{state | raft_ready_nodes: MapSet.delete(raft_ready_nodes, local)}
      end)

      :sys.replace_state(LeaseBootstrap, fn state ->
        now_ms = System.monotonic_time(:millisecond)

        state
        |> Map.put(:startup_complete?, true)
        |> Map.put(:startup_mode, :routing)
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
      assert body["mode"] == "routing_node"
      assert body["reason"] == "observer_sync"
    end

    test "demotes readiness immediately on write timeout outcome" do
      assert Ops.local_mode() == :routing_node

      local = Node.self()
      :ok = Ops.undrain_node(local)

      original_observer_state = :sys.get_state(Observer)
      original_bootstrap_state = :sys.get_state(LeaseBootstrap)

      on_exit(fn ->
        :sys.replace_state(Observer, fn _state -> original_observer_state end)
        :sys.replace_state(LeaseBootstrap, fn _state -> original_bootstrap_state end)
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

      :sys.replace_state(LeaseBootstrap, fn state ->
        now_ms = System.monotonic_time(:millisecond)

        state
        |> Map.put(:startup_complete?, true)
        |> Map.put(:startup_mode, :routing)
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
      assert conn.status == 200

      :ok = LeaseBootstrap.record_write_outcome(:leader_retry_timeout)

      eventually(fn ->
        assert %{consensus_probe_detail: %{probe_result: "write_timeout"}} =
                 :sys.get_state(LeaseBootstrap)
      end)

      conn = request("/health/ready")
      body = Jason.decode!(conn.resp_body)

      assert conn.status == 503
      assert body["reason"] == "lease_sync"
      assert body["detail"]["probe_result"] == "write_timeout"
      assert body["detail"]["outcome"] == "leader_retry_timeout"
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
      _ ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        else
          fun.()
        end
    end
  end

  describe "public endpoint" do
    test "does not expose health checks on the API port" do
      conn = public_request("/health/live")

      assert conn.status == 401
    end

    test "does not expose metrics on the API port" do
      conn = public_request("/metrics")

      assert conn.status == 401
    end

    test "does not expose pprof on the API port" do
      conn = public_request("/pprof")

      assert conn.status == 401
    end
  end

  describe "ops router" do
    test "exposes Prometheus metrics on the ops port" do
      conn = request("/metrics")

      assert conn.status == 200
      assert List.first(get_resp_header(conn, "content-type")) =~ "text/plain"
      assert conn.resp_body =~ "starcite"
    end

    test "exposes pprof on the ops port" do
      conn = request("/pprof")

      assert conn.status == 200
      assert conn.resp_body == "Welcome"
    end
  end
end
