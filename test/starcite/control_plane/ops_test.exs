defmodule Starcite.ControlPlane.OpsTest do
  use ExUnit.Case, async: false

  alias Starcite.ControlPlane.{Observer, Ops, WriteNodes}
  alias Starcite.DataPlane.{RaftBootstrap, RaftManager}

  setup do
    Ops.undrain_node(Node.self())

    on_exit(fn ->
      Ops.undrain_node(Node.self())
    end)

    :ok
  end

  test "status exposes write-node and observer snapshot" do
    status = Ops.status()

    assert status.node == Node.self()
    assert is_boolean(status.local_write_node)
    assert status.local_mode in [:write_node, :router_node]
    assert is_boolean(status.local_ready)
    assert is_boolean(status.local_drained)
    assert is_list(status.write_nodes)
    assert is_integer(status.write_replication_factor)
    assert status.write_replication_factor > 0
    assert is_integer(status.num_groups)
    assert status.num_groups > 0
    assert is_list(status.local_groups)
    assert is_list(status.local_leader_groups)
    assert is_map(status.raft_role_counts)
    assert status.raft_role_counts.leader >= 0
    assert is_map(status.raft_storage)
    assert is_binary(status.raft_storage.starcite_data_dir)
    assert is_binary(status.raft_storage.ra_data_dir)
    assert is_binary(status.raft_storage.ra_wal_data_dir)
    assert is_map(status.observer)
  end

  test "drain and undrain control routing eligibility" do
    local = Node.self()

    assert local in Observer.ready_nodes()
    assert {:error, :timeout} = Ops.wait_local_drained(50)

    :ok = Ops.drain_node(local)

    eventually(fn ->
      refute local in Observer.ready_nodes()
    end)

    assert :ok = Ops.wait_local_drained(1_000)

    :ok = Ops.undrain_node(local)

    eventually(fn ->
      assert local in Observer.ready_nodes()
    end)
  end

  test "local_drained tracks explicit drain status only" do
    local = Node.self()
    :ok = Ops.undrain_node(local)

    eventually(fn ->
      assert local in Observer.ready_nodes()
      refute Ops.local_drained()
    end)

    original_state = :sys.get_state(Observer)

    on_exit(fn ->
      :sys.replace_state(Observer, fn _state -> original_state end)
    end)

    :sys.replace_state(Observer, fn %{raft_ready_nodes: raft_ready_nodes} = state ->
      %{state | raft_ready_nodes: MapSet.delete(raft_ready_nodes, local)}
    end)

    refute local in Observer.ready_nodes()
    refute Ops.local_drained()
  end

  test "wait_local_ready succeeds once write-node convergence is restored" do
    local = Node.self()
    :ok = Ops.undrain_node(local)

    original_observer_state = :sys.get_state(Observer)
    original_bootstrap_state = :sys.get_state(RaftBootstrap)

    on_exit(fn ->
      :sys.replace_state(Observer, fn _state -> original_observer_state end)
      :sys.replace_state(RaftBootstrap, fn _state -> original_bootstrap_state end)
    end)

    :sys.replace_state(RaftBootstrap, fn state ->
      now_ms = System.monotonic_time(:millisecond)

      state
      |> Map.put(:startup_complete?, true)
      |> Map.put(:startup_mode, :write)
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

    :sys.replace_state(Observer, fn %{raft_ready_nodes: raft_ready_nodes} = state ->
      %{state | raft_ready_nodes: MapSet.delete(raft_ready_nodes, local)}
    end)

    assert Ops.wait_local_ready(100) in [:ok, {:error, :timeout}]

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

    eventually(fn ->
      send(Observer, :maintenance)
      assert :ok = Ops.wait_local_ready(500)
    end)
  end

  test "parse_known_node validates against known node set" do
    node_name = Node.self() |> Atom.to_string()

    assert {:ok, node} = Ops.parse_known_node(node_name)
    assert node == Node.self()

    assert {:error, :invalid_write_node} =
             Ops.parse_known_node("missing@starcite.internal")
  end

  test "parse_group_id enforces configured group bounds" do
    max_group_id = WriteNodes.num_groups() - 1

    assert {:ok, 0} = Ops.parse_group_id("0")
    assert {:ok, ^max_group_id} = Ops.parse_group_id(Integer.to_string(max_group_id))

    assert {:error, :invalid_group_id} =
             Ops.parse_group_id(Integer.to_string(max_group_id + 1))

    assert {:error, :invalid_group_id} = Ops.parse_group_id("-1")
    assert {:error, :invalid_group_id} = Ops.parse_group_id("not-a-number")
  end

  test "drain rejects nodes outside static write-node set" do
    non_write_node = :"router-1@starcite.internal"

    refute non_write_node in WriteNodes.nodes()
    assert {:error, :invalid_write_node} = Ops.drain_node(non_write_node)
    assert {:error, :invalid_write_node} = Ops.undrain_node(non_write_node)
  end

  test "leadership helpers expose local single-node state" do
    Enum.each(Ops.local_write_groups(), fn group_id ->
      assert :ok = RaftManager.start_group(group_id)
    end)

    eventually(
      fn ->
        assert Ops.raft_role_counts().leader == WriteNodes.num_groups()
      end,
      timeout: 5_000
    )

    states = Ops.local_raft_group_states()

    assert length(states) == WriteNodes.num_groups()
    assert Ops.local_leader_groups() == Ops.local_write_groups()
    assert Ops.group_leader(0) == Node.self()
    assert :ok = Ops.wait_group_leader(0, Node.self(), 1_000)
    assert :already_leader = Ops.transfer_group_leadership(0, Node.self())

    assert Ops.raft_role_counts() == %{
             leader: WriteNodes.num_groups(),
             follower: 0,
             candidate: 0,
             other: 0,
             down: 0
           }

    assert Enum.all?(states, fn state ->
             state.role == :leader and state.leader_node == Node.self() and
               state.replicas == [Node.self()]
           end)
  end

  test "transfer_group_leadership rejects targets outside the replica set" do
    assert {:error, :target_not_replica} =
             Ops.transfer_group_leadership(0, :"router-1@starcite.internal")
  end

  test "wait_group_leader rejects targets outside the replica set" do
    assert {:error, :target_not_replica} =
             Ops.wait_group_leader(0, :"router-1@starcite.internal", 100)
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
end
