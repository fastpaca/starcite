defmodule Starcite.DataPlane.SessionQuorumTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.SessionQuorum
  alias Starcite.Session

  setup do
    original_routing_node_ids = Application.get_env(:starcite, :routing_node_ids)
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
      Application.put_env(:starcite, :routing_node_ids, original_routing_node_ids)
      Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      :telemetry.detach(handler_id)
    end)

    :ok
  end

  test "succeeds locally when no remote acknowledgements are required" do
    Application.put_env(:starcite, :routing_node_ids, [Node.self()])
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

    Application.put_env(:starcite, :routing_node_ids, [Node.self(), missing])
    Application.put_env(:starcite, :routing_replication_factor, 2)

    session = Session.new("ses-repl-missing")

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
  alias Starcite.Session
  alias Starcite.TestSupport.DistributedPeerDataPlane

  @moduletag :distributed

  unless Node.alive?() do
    @moduletag skip: "requires distributed node (run tests with --sname or --name)"
  end

  setup do
    original_routing_node_ids = Application.get_env(:starcite, :routing_node_ids)
    original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)

    clear_local_data_plane()

    peers = start_peers(2)
    peer_nodes = Enum.map(peers, fn {_pid, node} -> node end)
    routing_nodes = [Node.self() | peer_nodes]

    Application.put_env(:starcite, :routing_node_ids, routing_nodes)
    Application.put_env(:starcite, :routing_replication_factor, 3)

    Enum.each(peers, fn {_peer_pid, peer_node} ->
      true = Node.connect(peer_node)
      :ok = add_code_paths(peer_node)
      :ok = configure_peer(peer_node, routing_nodes)
      :ok = start_peer_data_plane(peer_node)
    end)

    on_exit(fn ->
      clear_local_data_plane()

      Enum.each(peers, fn {peer_pid, peer_node} ->
        _ = stop_peer_data_plane(peer_node)
        _ = stop_peer(peer_pid)
      end)

      restore_env(:routing_node_ids, original_routing_node_ids)
      restore_env(:routing_replication_factor, original_replication_factor)
    end)

    {:ok, peers: peers}
  end

  test "replicates to every live standby before returning", %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-repl-dist-#{System.unique_integer([:positive, :monotonic])}"
    input = append_input("replicated-producer", 1)
    seed_session = Session.new(session_id)

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
    seed_session = Session.new(session_id)
    original_routing_node_ids = Application.get_env(:starcite, :routing_node_ids)
    original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)

    assert :ok = apply_local_and_replicate(seed_session, [])

    {:ok, session_one, [event_one]} =
      append_session(seed_session, append_input("bootstrap-writer", 1))

    assert :ok = apply_local_and_replicate(session_one, [event_one])

    assert :ok = :rpc.call(peer_b, SessionQuorum, :stop_session, [session_id], 5_000)
    Application.put_env(:starcite, :routing_node_ids, [Node.self(), peer_a])
    Application.put_env(:starcite, :routing_replication_factor, 2)

    {:ok, session_two, [event_two]} =
      append_session(session_one, append_input("bootstrap-writer", 2))

    assert :ok = apply_local_and_replicate(session_two, [event_two])

    {:ok, session_three, [event_three]} =
      append_session(session_two, append_input("bootstrap-writer", 3))

    assert :ok = apply_local_and_replicate(session_three, [event_three])
    Application.put_env(:starcite, :routing_node_ids, original_routing_node_ids)
    Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)

    eventually(fn ->
      assert_stored_session(peer_b, session_one)
      assert_peer_events(peer_b, session_id, session_one.epoch, [1])
    end)

    eventually(fn ->
      assert {:ok, ^session_three} =
               :rpc.call(peer_b, SessionQuorum, :get_session, [session_id], 5_000)

      assert_stored_session(peer_b, session_three)
      assert_peer_events(peer_b, session_id, session_three.epoch, [1, 2, 3])
    end)

    assert_peer_session(peer_a, session_three)
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

  defp configure_peer(peer_node, routing_nodes)
       when is_atom(peer_node) and is_list(routing_nodes) do
    :ok =
      :rpc.call(peer_node, Application, :put_env, [:starcite, :routing_node_ids, routing_nodes])

    :ok = :rpc.call(peer_node, Application, :put_env, [:starcite, :routing_replication_factor, 3])

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
    case :rpc.call(peer_node, DistributedPeerDataPlane, :start, [], 5_000) do
      :ok -> :ok
      other -> raise "failed to start peer data plane on #{peer_node}: #{inspect(other)}"
    end
  end

  defp stop_peer_data_plane(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, DistributedPeerDataPlane, :stop, [], 5_000) do
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

  defp clear_local_data_plane do
    :ok = SessionQuorum.clear()
    :ok = SessionStore.clear()
    :ok = EventStore.clear()
    :ok
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

  defp apply_local_and_replicate(%Session{} = session, events) when is_list(events) do
    with :ok <- SessionQuorum.apply_replica(session, events),
         :ok <- SessionQuorum.replicate_state(session, events) do
      :ok
    end
  end

  defp append_session(%Session{} = session, input) when is_map(input) do
    case Session.append_event(session, input) do
      {:appended, %Session{} = updated_session, event} ->
        {:ok, updated_session, [event]}

      other ->
        {:error, other}
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
