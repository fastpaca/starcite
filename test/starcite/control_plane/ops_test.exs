defmodule Starcite.ControlPlane.OpsTest do
  use ExUnit.Case, async: false

  alias Starcite.ControlPlane.{Observer, Ops, WriteNodes}
  alias Starcite.ControlPlane.Ops.Leadership
  alias Starcite.ControlPlane.{RaftBootstrap, RaftManager}

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

    assert Leadership.group_roles() == [:leader, :follower, :candidate, :other, :down]
    assert length(states) == WriteNodes.num_groups()
    assert Ops.local_leader_groups() == Ops.local_write_groups()
    assert Ops.group_leader(0) == Node.self()
    assert Leadership.local_group_probe(0) == %{role: :leader, running?: true}
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

  test "leadership helpers report down local groups and emit down-role telemetry" do
    ensure_all_local_groups_started()

    role_count_handler_id =
      "ops-raft-role-count-#{System.unique_integer([:positive, :monotonic])}"

    group_role_handler_id =
      "ops-raft-group-role-#{System.unique_integer([:positive, :monotonic])}"

    test_pid = self()

    :ok =
      :telemetry.attach(
        role_count_handler_id,
        [:starcite, :raft, :role_count],
        fn _event, measurements, metadata, pid ->
          send(pid, {:raft_role_count_event, measurements, metadata})
        end,
        test_pid
      )

    :ok =
      :telemetry.attach(
        group_role_handler_id,
        [:starcite, :raft, :group_role],
        fn _event, measurements, metadata, pid ->
          send(pid, {:raft_group_role_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(role_count_handler_id)
      :telemetry.detach(group_role_handler_id)
      restore_local_group(0)
    end)

    delete_local_group(0)

    eventually(
      fn ->
        state = find_local_group_state!(0)

        assert state.role == :down
        assert state.leader_node == nil
        refute 0 in Ops.local_leader_groups()

        assert Ops.raft_role_counts() == %{
                 leader: WriteNodes.num_groups() - 1,
                 follower: 0,
                 candidate: 0,
                 other: 0,
                 down: 1
               }
      end,
      timeout: 5_000
    )

    assert Ops.group_leader(0) == nil
    assert Leadership.local_group_probe(0) == %{role: :down, running?: false}
    assert {:error, :timeout} = Ops.wait_group_leader(0, Node.self(), 200)

    assert :ok = Leadership.emit_local_raft_role_telemetry()
    assert_receive_raft_role_count(:down, 1, Node.self())

    assert :ok = Leadership.emit_local_raft_role_telemetry()
    assert_receive_raft_role_count(:leader, WriteNodes.num_groups() - 1, Node.self())

    assert :ok = Leadership.emit_local_raft_role_telemetry()
    assert_receive_raft_group_role(0, :down, 1, Node.self())

    assert :ok = Leadership.emit_local_raft_role_telemetry()
    assert_receive_raft_group_role(0, :leader, 0, Node.self())
  end

  test "transfer_group_leadership emits normalized telemetry for error paths" do
    ensure_all_local_groups_started()

    handler_id = "ops-raft-leadership-transfer-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :raft, :leadership_transfer],
        fn _event, measurements, metadata, pid ->
          send(pid, {:raft_leadership_transfer_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      restore_local_group(0)
    end)

    assert {:error, :target_not_replica} =
             Ops.transfer_group_leadership(0, :"router-1@starcite.internal")

    assert_receive_raft_leadership_transfer(
      0,
      :error,
      :target_not_replica,
      Atom.to_string(Node.self()),
      "router-1@starcite.internal"
    )

    delete_local_group(0)

    eventually(
      fn ->
        assert Ops.group_leader(0) == nil
      end,
      timeout: 5_000
    )

    assert {:error, :leader_unknown} = Ops.transfer_group_leadership(0, Node.self())

    assert_receive_raft_leadership_transfer(
      0,
      :error,
      :leader_unknown,
      "unknown",
      Atom.to_string(Node.self())
    )

    invalid_group_id = WriteNodes.num_groups()

    assert {:error, :invalid_group_id} =
             Ops.transfer_group_leadership(invalid_group_id, Node.self())

    assert_receive_raft_leadership_transfer(
      invalid_group_id,
      :error,
      :invalid_group_id,
      "unknown",
      Atom.to_string(Node.self())
    )
  end

  test "leadership helpers reject invalid public arguments" do
    invalid_group_id = WriteNodes.num_groups()

    assert_raise ArgumentError, ~r/invalid group_id/, fn ->
      Ops.group_leader(invalid_group_id)
    end

    assert {:error, :invalid_args} = Ops.transfer_group_leadership("0", Node.self())
    assert {:error, :invalid_args} = Ops.wait_group_leader(0, Node.self(), 0)
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

  defp ensure_all_local_groups_started do
    Enum.each(Ops.local_write_groups(), fn group_id ->
      assert :ok = RaftManager.start_group(group_id)
    end)

    eventually(
      fn ->
        assert Ops.raft_role_counts().leader == WriteNodes.num_groups()
      end,
      timeout: 5_000
    )
  end

  defp delete_local_group(group_id) when is_integer(group_id) and group_id >= 0 do
    server_ref = {RaftManager.server_id(group_id), Node.self()}

    case :ra.delete_cluster([server_ref], 5_000) do
      {:ok, _members} ->
        :ok

      {:error, _reason} ->
        case :ra.force_delete_server(:default, server_ref) do
          :ok ->
            :ok

          {:error, reason} ->
            flunk("failed to delete local raft group #{group_id}: #{inspect(reason)}")
        end
    end
  end

  defp restore_local_group(group_id) when is_integer(group_id) and group_id >= 0 do
    assert :ok = RaftManager.start_group(group_id)

    eventually(
      fn ->
        assert Ops.group_leader(group_id) == Node.self()
      end,
      timeout: 5_000
    )
  end

  defp find_local_group_state!(group_id) when is_integer(group_id) and group_id >= 0 do
    Ops.local_raft_group_states()
    |> Enum.find(&match?(%{group_id: ^group_id}, &1))
    |> case do
      nil -> flunk("missing local group state for #{group_id}")
      state -> state
    end
  end

  defp assert_receive_raft_role_count(role, groups, node_name)
       when role in [:leader, :follower, :candidate, :other, :down] and
              is_integer(groups) and groups >= 0 and is_atom(node_name) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_raft_role_count(role, groups, Atom.to_string(node_name), deadline)
  end

  defp assert_receive_raft_group_role(group_id, role, present, node_name)
       when is_integer(group_id) and group_id >= 0 and
              role in [:leader, :follower, :candidate, :other, :down] and
              present in [0, 1] and is_atom(node_name) do
    deadline = System.monotonic_time(:millisecond) + 1_000

    do_assert_receive_raft_group_role(
      group_id,
      role,
      present,
      Atom.to_string(node_name),
      deadline
    )
  end

  defp assert_receive_raft_leadership_transfer(
         group_id,
         outcome,
         reason,
         source_node,
         target_node
       )
       when is_integer(group_id) and group_id >= 0 and
              outcome in [:ok, :already_leader, :error, :timeout] and is_atom(reason) and
              is_binary(source_node) and is_binary(target_node) do
    deadline = System.monotonic_time(:millisecond) + 1_000

    do_assert_receive_raft_leadership_transfer(
      group_id,
      outcome,
      reason,
      source_node,
      target_node,
      deadline
    )
  end

  defp do_assert_receive_raft_role_count(role, groups, node_name, deadline)
       when is_binary(node_name) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:raft_role_count_event, %{groups: ^groups}, %{node: ^node_name, role: ^role}} ->
        :ok

      {:raft_role_count_event, _measurements, _metadata} ->
        do_assert_receive_raft_role_count(role, groups, node_name, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for raft role count telemetry role=#{inspect(role)} groups=#{inspect(groups)} node=#{inspect(node_name)}"
        )
    end
  end

  defp do_assert_receive_raft_group_role(group_id, role, present, node_name, deadline)
       when is_binary(node_name) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:raft_group_role_event, %{present: ^present},
       %{node: ^node_name, group_id: ^group_id, role: ^role}} ->
        :ok

      {:raft_group_role_event, _measurements, _metadata} ->
        do_assert_receive_raft_group_role(group_id, role, present, node_name, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for raft group role telemetry group_id=#{inspect(group_id)} role=#{inspect(role)} present=#{inspect(present)} node=#{inspect(node_name)}"
        )
    end
  end

  defp do_assert_receive_raft_leadership_transfer(
         group_id,
         outcome,
         reason,
         source_node,
         target_node,
         deadline
       ) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:raft_leadership_transfer_event, %{count: 1},
       %{
         group_id: ^group_id,
         outcome: ^outcome,
         reason: ^reason,
         source_node: ^source_node,
         target_node: ^target_node
       }} ->
        :ok

      {:raft_leadership_transfer_event, _measurements, _metadata} ->
        do_assert_receive_raft_leadership_transfer(
          group_id,
          outcome,
          reason,
          source_node,
          target_node,
          deadline
        )
    after
      remaining ->
        flunk(
          "timed out waiting for leadership transfer telemetry group_id=#{inspect(group_id)} outcome=#{inspect(outcome)} reason=#{inspect(reason)} source_node=#{inspect(source_node)} target_node=#{inspect(target_node)}"
        )
    end
  end
end
