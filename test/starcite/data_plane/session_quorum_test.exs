defmodule Starcite.DataPlane.SessionQuorumTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.SessionQuorum
  alias Starcite.Routing.Store
  alias Starcite.Session

  setup do
    original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
    original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)
    handler_id = "session-replication-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :replication, :session],
        fn _event, measurements, metadata, pid ->
          send(pid, {:session_replication_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
      Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      :telemetry.detach(handler_id)
    end)

    :ok
  end

  test "succeeds locally when no remote acknowledgements are required" do
    Application.put_env(:starcite, :cluster_node_ids, [Node.self()])
    Application.put_env(:starcite, :routing_replication_factor, 1)

    session = Session.new("ses-repl-local")

    assert :ok = SessionQuorum.replicate_state(session, [])

    assert_receive {:session_replication_event, measurements, metadata}, 1_000
    assert measurements.count == 1
    assert measurements.standby_count == 0
    assert measurements.required_remote_acks == 0
    assert measurements.successful_remote_acks == 0
    assert measurements.failure_count == 0
    assert is_integer(measurements.duration_ms)
    assert measurements.duration_ms >= 0

    assert metadata.session_id == "ses-repl-local"
    assert metadata.tenant_id == "service"
    assert metadata.outcome == :ok
    assert metadata.failure_reason == :none
  end

  test "returns quorum error when required remote acknowledgement cannot be obtained" do
    missing = :"missing-replica@127.0.0.1"
    session_id = "ses-repl-missing"

    Application.put_env(:starcite, :cluster_node_ids, [Node.self(), missing])
    Application.put_env(:starcite, :routing_replication_factor, 2)

    session = Session.new(session_id)

    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               %{
                 owner: Node.self(),
                 epoch: 1,
                 replicas: [Node.self(), missing],
                 status: :active,
                 updated_at_ms: System.system_time(:millisecond)
               },
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    assert {:error, {:replication_quorum_not_met, details}} =
             SessionQuorum.replicate_state(session, [])

    assert details.required_remote_acks == 1
    assert details.successful_remote_acks == 0
    assert details.failures != []

    assert_receive {:session_replication_event, measurements, metadata}, 1_000
    assert measurements.count == 1
    assert measurements.standby_count == 1
    assert measurements.required_remote_acks == 1
    assert measurements.successful_remote_acks == 0
    assert measurements.failure_count >= 1
    assert is_integer(measurements.duration_ms)
    assert measurements.duration_ms >= 0

    assert metadata.session_id == "ses-repl-missing"
    assert metadata.tenant_id == "service"
    assert metadata.outcome == :quorum_not_met
    assert metadata.failure_reason in [:badrpc, :timeout, :error]
  end
end

defmodule Starcite.DataPlane.SessionQuorumDistributedTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.{EventStore, SessionQuorum, SessionStore}
  alias Starcite.Operations
  alias Starcite.Routing.Store
  alias Starcite.Session
  alias Starcite.WritePath
  alias Starcite.TestSupport.DistributedPeerDataPlane

  @moduletag :distributed

  unless Node.alive?() do
    @moduletag skip: "requires distributed node (run tests with --sname or --name)"
  end

  setup_all do
    original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
    original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)
    original_routing_store_dir = Application.get_env(:starcite, :routing_store_dir)

    routing_store_dir =
      Path.join(
        System.tmp_dir!(),
        "starcite-dist-#{System.unique_integer([:positive, :monotonic])}"
      )

    peers = start_peers(2)
    peer_nodes = Enum.map(peers, fn {_pid, node} -> node end)
    cluster_nodes = [Node.self() | peer_nodes]

    Application.put_env(:starcite, :routing_store_dir, routing_store_dir)
    Application.put_env(:starcite, :cluster_node_ids, cluster_nodes)
    Application.put_env(:starcite, :routing_replication_factor, 3)

    Enum.with_index(peers, 2)
    |> Enum.each(fn {{_peer_pid, peer_node}, expected_cluster_size} ->
      true = Node.connect(peer_node)
      :ok = add_code_paths(peer_node)
      :ok = configure_peer(peer_node, cluster_nodes, routing_store_dir)
      :ok = start_peer_data_plane(peer_node)
      :ok = wait_for_cluster_size(expected_cluster_size)
    end)

    on_exit(fn ->
      clear_local_data_plane()

      Enum.each(peers, fn {peer_pid, peer_node} ->
        _ = stop_peer_data_plane(peer_node)
        _ = stop_peer(peer_pid)
      end)

      restore_env(:routing_store_dir, original_routing_store_dir)
      restore_env(:cluster_node_ids, original_cluster_node_ids)
      restore_env(:routing_replication_factor, original_replication_factor)
      File.rm_rf(routing_store_dir)
    end)

    {:ok, peers: peers}
  end

  setup %{peers: peers} do
    clear_local_data_plane()
    :ok = Store.clear()
    :ok = Store.renew_local_lease()

    Enum.each(peers, fn {_peer_pid, peer_node} ->
      :ok = start_peer_data_plane(peer_node)
      sync_peer_lease_ttl(peer_node)
    end)

    :ok = wait_for_cluster_size(length(peers) + 1)

    Enum.each(peers, fn {_peer_pid, peer_node} ->
      assert :ok = :rpc.call(peer_node, DistributedPeerDataPlane, :clear, [], 15_000)
      assert :ok = :rpc.call(peer_node, Store, :renew_local_lease, [], 5_000)
    end)

    :ok
  end

  test "replicates to every live standby before returning", %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-repl-dist-#{System.unique_integer([:positive, :monotonic])}"
    input = append_input("replicated-producer", 1)
    seed_session = Session.new(session_id)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = SessionQuorum.replicate_state(seed_session, [])
    assert {:appended, updated_session, event} = Session.append_event(seed_session, input)

    replicated_session = %Session{updated_session | epoch: 7}
    assert :ok = SessionQuorum.replicate_state(replicated_session, [event])

    eventually(fn ->
      assert_peer_session(peer_a, replicated_session)
      assert_peer_session(peer_b, replicated_session)
      assert_peer_events(peer_a, session_id, replicated_session.epoch, [1])
      assert_peer_events(peer_b, session_id, replicated_session.epoch, [1])
    end)
  end

  test "log bootstrap prefers fresher peer state over stale local cache", %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-repl-bootstrap-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = %Session{Session.new(session_id) | epoch: 1}

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = seed_replica_set(Node.self(), seed_session, [])

    assert {:ok, reply_one} =
             WritePath.append_event(session_id, append_input("bootstrap-writer", 1))

    assert reply_one.seq == 1

    eventually(fn ->
      assert {:ok, session} = SessionQuorum.get_session(session_id)
      assert session.last_seq == 1
    end)

    assert {:ok, session_one} = SessionQuorum.get_session(session_id)
    [event_one] = EventStore.from_cursor(session_id, 0, 1)
    assert :ok = :rpc.call(peer_b, SessionQuorum, :stop_session, [session_id], 5_000)
    assert :ok = seed_peer_cache(peer_b, session_one, [event_one])

    put_assignment(session_id, Node.self(), [Node.self(), peer_a])

    eventually(fn ->
      assert {:ok, session_one} =
               :rpc.call(peer_b, SessionStore, :get_session_cached, [session_id], 5_000)

      assert session_one.last_seq == 1
      assert_stored_session(peer_b, session_one)
      assert_peer_events(peer_b, session_id, session_one.epoch, [1])
    end)

    assert {:ok, reply_two} =
             WritePath.append_event(session_id, append_input("bootstrap-writer", 2))

    assert reply_two.seq == 2

    assert {:ok, reply_three} =
             WritePath.append_event(session_id, append_input("bootstrap-writer", 3))

    assert reply_three.seq == 3

    assert {:ok, session_three} = SessionQuorum.get_session(session_id)
    assert session_three.last_seq == 3

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])

    eventually(fn ->
      assert {:ok, ^session_three} =
               :rpc.call(peer_b, SessionQuorum, :get_session, [session_id], 5_000)

      assert_stored_session(peer_b, session_three)
      assert_peer_events(peer_b, session_id, session_three.epoch, [1, 2, 3])
    end)

    assert_peer_session(peer_a, session_three)
  end

  test "manual drain hands ownership to a replica and waits for the node to drain", %{
    peers: peers
  } do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-drain-dist-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = Session.new(session_id)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = seed_replica_set(Node.self(), seed_session, [])

    {:ok, append_reply} = WritePath.append_event(session_id, append_input("drain-writer", 1))
    assert append_reply.seq == 1

    assert :ok = Operations.drain_node(Node.self())
    assert :ok = Operations.wait_local_drained(5_000)

    eventually(fn ->
      assert {:ok, %{owner: ^peer_a, epoch: 2, status: :active}} =
               Store.get_assignment(session_id, favor: :consistency)

      assert false == SessionQuorum.local_owner?(session_id)
      assert true == :rpc.call(peer_a, SessionQuorum, :local_owner?, [session_id], 5_000)
    end)

    {:ok, routed_reply} = WritePath.append_event(session_id, append_input("drain-writer", 2))
    assert routed_reply.seq == 2

    eventually(fn ->
      assert {:ok, session} = :rpc.call(peer_a, SessionQuorum, :get_session, [session_id], 5_000)
      assert session.last_seq == 2
      assert_peer_events(peer_a, session_id, session.epoch, [1, 2])
    end)
  end

  test "lease expiry triggers failover without using nodedown as authority", %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-lease-failover-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = Session.new(session_id)
    original_ttl = Application.get_env(:starcite, :routing_lease_ttl_ms)

    on_exit(fn ->
      restore_env(:routing_lease_ttl_ms, original_ttl)

      Enum.each([peer_a, peer_b], fn peer_node ->
        _ = restore_peer_env(peer_node, :routing_lease_ttl_ms, original_ttl)
      end)
    end)

    Application.put_env(:starcite, :routing_lease_ttl_ms, 300)

    Enum.each([peer_a, peer_b], fn peer_node ->
      assert :ok =
               :rpc.call(
                 peer_node,
                 Application,
                 :put_env,
                 [:starcite, :routing_lease_ttl_ms, 300],
                 5_000
               )
    end)

    put_assignment(session_id, peer_a, [peer_a, Node.self(), peer_b])
    assert :ok = seed_replica_set(peer_a, seed_session, [])

    assert {:ok, reply} =
             :rpc.call(
               peer_a,
               WritePath,
               :append_event,
               [session_id, append_input("lease-writer", 1)],
               5_000
             )

    assert reply.seq == 1

    assert :ok = stop_peer_data_plane(peer_a)

    eventually(
      fn ->
        assert {:ok, %{owner: owner, epoch: 2, status: :active}} =
                 Store.get_assignment(session_id, favor: :consistency)

        assert owner in [Node.self(), peer_b]
        assert owner != peer_a
      end,
      timeout: 6_000
    )
  end

  test "lease expiry promotes a moving target owner and preserves writes", %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-moving-failover-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = Session.new(session_id)
    original_ttl = Application.get_env(:starcite, :routing_lease_ttl_ms)

    on_exit(fn ->
      restore_env(:routing_lease_ttl_ms, original_ttl)

      Enum.each([peer_a, peer_b], fn peer_node ->
        _ = restore_peer_env(peer_node, :routing_lease_ttl_ms, original_ttl)
      end)
    end)

    Application.put_env(:starcite, :routing_lease_ttl_ms, 300)

    Enum.each([peer_a, peer_b], fn peer_node ->
      assert :ok =
               :rpc.call(
                 peer_node,
                 Application,
                 :put_env,
                 [:starcite, :routing_lease_ttl_ms, 300],
                 5_000
               )
    end)

    assert :ok =
             seed_moving_assignment(session_id, peer_a, Node.self(), [peer_a, Node.self(), peer_b])

    assert :ok = seed_replica_set(peer_a, seed_session, [])

    assert {:ok, reply} =
             :rpc.call(
               peer_a,
               WritePath,
               :append_event,
               [session_id, append_input("moving-writer", 1)],
               5_000
             )

    assert reply.seq == 1

    assert :ok = stop_peer_data_plane(peer_a)

    eventually(
      fn ->
        assert {:ok, %{owner: owner, epoch: 2, status: :active}} =
                 Store.get_assignment(session_id, favor: :consistency)

        assert owner == Node.self()
        assert true == SessionQuorum.local_owner?(session_id)
      end,
      timeout: 6_000
    )

    assert {:ok, routed_reply} =
             WritePath.append_event(session_id, append_input("moving-writer", 2))

    assert routed_reply.seq == 2

    eventually(fn ->
      assert {:ok, session} = SessionQuorum.get_session(session_id)
      assert session.last_seq == 2
      assert_peer_events(peer_b, session_id, session.epoch, [1, 2])
    end)
  end

  defp start_peers(count) when is_integer(count) and count > 0 do
    Enum.map(1..count, fn _index ->
      start_named_peer(:"replication_peer_#{System.unique_integer([:positive])}")
    end)
  end

  defp start_named_peer(name) when is_atom(name) do
    {:ok, pid, node} = :peer.start_link(%{name: name})
    {pid, node}
  end

  defp add_code_paths(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, :code, :add_paths, [:code.get_path()]) do
      :ok -> :ok
      other -> raise "failed to add code paths on #{peer_node}: #{inspect(other)}"
    end
  end

  defp configure_peer(peer_node, cluster_nodes, routing_store_dir)
       when is_atom(peer_node) and is_list(cluster_nodes) and is_binary(routing_store_dir) do
    routing_lease_ttl_ms = Application.get_env(:starcite, :routing_lease_ttl_ms, 5_000)

    :ok =
      :rpc.call(
        peer_node,
        Application,
        :put_env,
        [:starcite, :routing_store_dir, routing_store_dir]
      )

    :ok =
      :rpc.call(peer_node, Application, :put_env, [:starcite, :cluster_node_ids, cluster_nodes])

    :ok = :rpc.call(peer_node, Application, :put_env, [:starcite, :routing_replication_factor, 3])

    :ok =
      :rpc.call(
        peer_node,
        Application,
        :put_env,
        [:starcite, :routing_lease_ttl_ms, routing_lease_ttl_ms]
      )

    :ok =
      :rpc.call(
        peer_node,
        Application,
        :put_env,
        [:starcite, :archive_adapter, Starcite.Archive.Adapter.Postgres]
      )

    :ok
  end

  defp start_peer_data_plane(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, DistributedPeerDataPlane, :start, [], 15_000) do
      :ok -> :ok
      other -> raise "failed to start peer data plane on #{peer_node}: #{inspect(other)}"
    end
  end

  defp stop_peer_data_plane(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, DistributedPeerDataPlane, :stop, [], 15_000) do
      :ok -> :ok
      {:badrpc, _reason} -> :ok
      _other -> :ok
    end
  end

  defp stop_peer(peer_pid) when is_pid(peer_pid) do
    :peer.stop(peer_pid)
  catch
    :exit, _reason -> :ok
  end

  defp sync_peer_lease_ttl(peer_node) when is_atom(peer_node) do
    lease_ttl_ms = Application.get_env(:starcite, :routing_lease_ttl_ms, 5_000)

    assert :ok =
             :rpc.call(
               peer_node,
               Application,
               :put_env,
               [:starcite, :routing_lease_ttl_ms, lease_ttl_ms],
               5_000
             )

    :ok
  end

  defp clear_local_data_plane do
    :ok = SessionQuorum.clear()
    :ok = SessionStore.clear()
    :ok = EventStore.clear()
    :ok
  end

  defp wait_for_cluster_size(expected_size)
       when is_integer(expected_size) and expected_size > 0 do
    eventually(
      fn ->
        assert {:ok, nodes} = :khepri_cluster.nodes(Store.store_id(), %{favor: :consistency})
        assert length(nodes) == expected_size
        assert Node.self() in nodes
      end,
      timeout: 15_000,
      interval: 100
    )

    :ok
  end

  defp put_assignment(session_id, owner, replicas)
       when is_binary(session_id) and is_atom(owner) and is_list(replicas) do
    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               %{
                 owner: owner,
                 epoch: 1,
                 replicas: replicas,
                 status: :active,
                 updated_at_ms: System.system_time(:millisecond)
               },
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    :ok
  end

  defp seed_moving_assignment(session_id, owner, target_owner, replicas)
       when is_binary(session_id) and is_atom(owner) and is_atom(target_owner) and
              is_list(replicas) do
    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               %{
                 owner: owner,
                 epoch: 1,
                 replicas: replicas,
                 status: :moving,
                 target_owner: target_owner,
                 transfer_id: "xfer-#{System.unique_integer([:positive, :monotonic])}",
                 updated_at_ms: System.system_time(:millisecond)
               },
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    :ok
  end

  defp seed_replica_set(owner_node, %Session{} = session, events) when is_list(events) do
    if owner_node == Node.self() do
      with :ok <- SessionQuorum.apply_replica(session, events),
           :ok <- SessionQuorum.replicate_state(session, events) do
        :ok
      end
    else
      with :ok <- :rpc.call(owner_node, SessionQuorum, :apply_replica, [session, events], 5_000),
           :ok <- :rpc.call(owner_node, SessionQuorum, :replicate_state, [session, events], 5_000) do
        :ok
      end
    end
  end

  defp assert_peer_session(peer_node, %Session{id: session_id} = expected_session)
       when is_atom(peer_node) and is_binary(session_id) do
    case :rpc.call(peer_node, SessionQuorum, :get_session, [session_id], 5_000) do
      {:ok, %Session{} = actual_session} ->
        assert actual_session == expected_session

      other ->
        raise "unexpected session state on #{peer_node}: #{inspect(other)}"
    end
  end

  defp assert_peer_events(peer_node, session_id, epoch, expected_seqs)
       when is_atom(peer_node) and is_binary(session_id) and is_integer(epoch) and epoch >= 0 and
              is_list(expected_seqs) do
    case :rpc.call(peer_node, EventStore, :from_cursor, [session_id, 0, 10], 5_000) do
      events when is_list(events) ->
        assert Enum.map(events, & &1.seq) == expected_seqs
        assert Enum.all?(events, &(&1.epoch == epoch))

      other ->
        raise "unexpected event state on #{peer_node}: #{inspect(other)}"
    end
  end

  defp assert_stored_session(peer_node, %Session{id: session_id} = expected_session)
       when is_atom(peer_node) and is_binary(session_id) do
    case :rpc.call(peer_node, SessionStore, :get_session_cached, [session_id], 5_000) do
      {:ok, %Session{} = actual_session} ->
        assert actual_session == expected_session

      other ->
        raise "unexpected cached session state on #{peer_node}: #{inspect(other)}"
    end
  end

  defp seed_peer_cache(
         peer_node,
         %Session{id: session_id, tenant_id: tenant_id} = session,
         events
       )
       when is_atom(peer_node) and is_binary(session_id) and is_binary(tenant_id) and
              is_list(events) do
    with :ok <- :rpc.call(peer_node, SessionStore, :put_session, [session], 5_000),
         :ok <-
           :rpc.call(peer_node, EventStore, :put_events, [session_id, tenant_id, events], 5_000) do
      :ok
    else
      other ->
        raise "failed to seed peer cache on #{peer_node}: #{inspect(other)}"
    end
  end

  defp append_input(producer_id, producer_seq)
       when is_binary(producer_id) and producer_id != "" and is_integer(producer_seq) and
              producer_seq > 0 do
    %{
      type: "message",
      payload: %{"text" => "replicated"},
      actor: "user",
      source: "test",
      metadata: %{"path" => "replication"},
      refs: %{},
      idempotency_key: "key-#{producer_id}-#{producer_seq}",
      producer_id: producer_id,
      producer_seq: producer_seq
    }
  end

  defp restore_env(key, nil) when is_atom(key), do: Application.delete_env(:starcite, key)
  defp restore_env(key, value) when is_atom(key), do: Application.put_env(:starcite, key, value)

  defp restore_peer_env(peer_node, key, nil) when is_atom(peer_node) and is_atom(key) do
    :rpc.call(peer_node, Application, :delete_env, [:starcite, key], 5_000)
  end

  defp restore_peer_env(peer_node, key, value) when is_atom(peer_node) and is_atom(key) do
    :rpc.call(peer_node, Application, :put_env, [:starcite, key, value], 5_000)
  end

  defp eventually(fun, opts \\ []) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 3_000)
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
