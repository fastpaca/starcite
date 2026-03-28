defmodule Starcite.Routing.SessionRouterDistributedTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.{EventStore, SessionQuorum, SessionStore}
  alias Starcite.Routing.{SessionRouter, Store}
  alias Starcite.TestSupport.{DistributedPeerDataPlane, DistributedRoutingTarget}

  @moduletag :distributed
  @response_key :distributed_routing_target_response

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
        "starcite-router-dist-#{System.unique_integer([:positive, :monotonic])}"
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
      :ok = start_peer_runtime(peer_node)
      :ok = wait_for_cluster_size(expected_cluster_size)
    end)

    on_exit(fn ->
      clear_local_runtime()

      Enum.each(peers, fn {peer_pid, peer_node} ->
        _ = stop_peer_runtime(peer_node)
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
    clear_local_runtime()
    :ok = Store.clear()
    :ok = Store.renew_local_lease()
    Application.delete_env(:starcite, @response_key)

    Enum.each(peers, fn {_peer_pid, peer_node} ->
      :ok = start_peer_runtime(peer_node)
      :ok = sync_peer_lease_ttl(peer_node)
    end)

    :ok = wait_for_cluster_size(length(peers) + 1)

    Enum.each(peers, fn {_peer_pid, peer_node} ->
      assert :ok = :rpc.call(peer_node, DistributedPeerDataPlane, :clear, [], 15_000)
      assert :ok = :rpc.call(peer_node, Store, :renew_local_lease, [], 5_000)

      assert :ok =
               :rpc.call(peer_node, Application, :delete_env, [:starcite, @response_key], 5_000)
    end)

    :ok
  end

  test "refreshes the authoritative assignment on redirect before rerouting to the new owner",
       %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "router-dist-refresh-#{System.unique_integer([:positive, :monotonic])}"

    stale_assignment = %{owner: peer_a, epoch: 1, status: :active}

    put_assignment_record(session_id, %{
      owner: peer_b,
      epoch: 2,
      replicas: [peer_a, peer_b, Node.self()],
      status: :active,
      updated_at_ms: System.system_time(:millisecond)
    })

    assert :ok =
             :rpc.call(
               peer_a,
               Application,
               :put_env,
               [:starcite, @response_key, {:redirect, peer_b}],
               5_000
             )

    assert :ok =
             :rpc.call(
               peer_b,
               Application,
               :put_env,
               [:starcite, @response_key, {:ok, {:routed, peer_b}}],
               5_000
             )

    assert {:ok, {:routed, ^peer_b}} =
             SessionRouter.call(
               session_id,
               DistributedRoutingTarget,
               :dispatch,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: stale_assignment
             )
  end

  test "returns ownership_transfer_in_progress when the authoritative assignment is moving",
       %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "router-dist-moving-#{System.unique_integer([:positive, :monotonic])}"
    transfer_id = "xfer-#{System.unique_integer([:positive, :monotonic])}"

    stale_assignment = %{owner: peer_a, epoch: 1, status: :active}

    put_assignment_record(session_id, %{
      owner: peer_a,
      epoch: 2,
      replicas: [peer_a, peer_b, Node.self()],
      status: :moving,
      target_owner: peer_b,
      transfer_id: transfer_id,
      updated_at_ms: System.system_time(:millisecond)
    })

    assert :ok =
             :rpc.call(
               peer_a,
               Application,
               :put_env,
               [:starcite, @response_key, {:redirect, peer_b}],
               5_000
             )

    assert {:error, :ownership_transfer_in_progress} =
             SessionRouter.call(
               session_id,
               DistributedRoutingTarget,
               :dispatch,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: stale_assignment
             )
  end

  test "rejects redirect hints when the authoritative assignment is unchanged", %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "router-dist-stale-#{System.unique_integer([:positive, :monotonic])}"

    stale_assignment = %{owner: peer_a, epoch: 1, status: :active}

    put_assignment_record(session_id, %{
      owner: peer_a,
      epoch: 1,
      replicas: [peer_a, peer_b, Node.self()],
      status: :active,
      updated_at_ms: System.system_time(:millisecond)
    })

    assert :ok =
             :rpc.call(
               peer_a,
               Application,
               :put_env,
               [:starcite, @response_key, {:redirect, peer_b}],
               5_000
             )

    assert {:error, {:routing_rpc_failed, ^peer_a, :stale_assignment}} =
             SessionRouter.call(
               session_id,
               DistributedRoutingTarget,
               :dispatch,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: stale_assignment
             )
  end

  def local_echo, do: {:ok, :local}

  defp start_peers(count) when is_integer(count) and count > 0 do
    Enum.map(1..count, fn _index ->
      start_named_peer(:"router_peer_#{System.unique_integer([:positive])}")
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
    event_archive_opts = Application.get_env(:starcite, :event_archive_opts, [])

    assert :ok =
             :rpc.call(
               peer_node,
               Application,
               :put_env,
               [:starcite, :routing_store_dir, routing_store_dir]
             )

    assert :ok =
             :rpc.call(
               peer_node,
               Application,
               :put_env,
               [:starcite, :cluster_node_ids, cluster_nodes]
             )

    assert :ok =
             :rpc.call(
               peer_node,
               Application,
               :put_env,
               [:starcite, :routing_replication_factor, 3]
             )

    assert :ok =
             :rpc.call(
               peer_node,
               Application,
               :put_env,
               [:starcite, :routing_lease_ttl_ms, routing_lease_ttl_ms]
             )

    assert :ok =
             :rpc.call(
               peer_node,
               Application,
               :put_env,
               [:starcite, :event_archive_opts, event_archive_opts]
             )

    :ok
  end

  defp start_peer_runtime(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, DistributedPeerDataPlane, :start, [], 15_000) do
      :ok -> :ok
      other -> raise "failed to start peer runtime on #{peer_node}: #{inspect(other)}"
    end
  end

  defp stop_peer_runtime(peer_node) when is_atom(peer_node) do
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

  defp clear_local_runtime do
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

  defp put_assignment_record(session_id, assignment)
       when is_binary(session_id) and session_id != "" and is_map(assignment) do
    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               assignment,
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    :ok
  end

  defp restore_env(key, nil) when is_atom(key), do: Application.delete_env(:starcite, key)
  defp restore_env(key, value) when is_atom(key), do: Application.put_env(:starcite, key, value)

  defp eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
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
