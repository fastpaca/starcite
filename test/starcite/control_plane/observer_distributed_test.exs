defmodule Starcite.ControlPlane.ObserverDistributedTest do
  use ExUnit.Case, async: false

  alias Starcite.ControlPlane.{Observer, Ops}

  @moduletag :distributed

  unless Node.alive?() do
    @moduletag skip: "requires distributed node (run tests with --sname or --name)"
  end

  setup do
    previous_write_node_ids = Application.get_env(:starcite, :write_node_ids)
    previous_replication_factor = Application.get_env(:starcite, :write_replication_factor)

    peers = start_peers(2)
    peer_nodes = Enum.map(peers, fn {_pid, node} -> node end)
    write_nodes = [Node.self() | peer_nodes]

    Application.put_env(:starcite, :write_node_ids, write_nodes)
    Application.put_env(:starcite, :write_replication_factor, 1)

    Enum.each(peers, fn {_peer_pid, peer_node} ->
      true = Node.connect(peer_node)

      :ok = add_code_paths(peer_node)
      :ok = configure_peer(peer_node, write_nodes)
      :ok = start_peer_observer(peer_node)
    end)

    on_exit(fn ->
      _ = Ops.undrain_node(Node.self())

      Enum.each(peers, fn {peer_pid, peer_node} ->
        _ = stop_peer_observer(peer_node)
        _ = stop_peer(peer_pid)
      end)

      restore_env(:write_node_ids, previous_write_node_ids)
      restore_env(:write_replication_factor, previous_replication_factor)
    end)

    {:ok, peers: peers, write_nodes: write_nodes}
  end

  test "drain propagates cluster-wide and survives observer restart", %{peers: peers} do
    [peer_a, peer_b] = Enum.map(peers, fn {_pid, node} -> node end)
    local = Node.self()

    :ok = Ops.undrain_node(local)
    :ok = Ops.drain_node(local)

    eventually(fn ->
      assert remote_node_status(peer_a, local) == :draining
      assert remote_node_status(peer_b, local) == :draining
    end)

    :ok = stop_peer_observer(peer_a)
    :ok = start_peer_observer(peer_a)

    eventually(fn ->
      assert remote_node_status(peer_a, local) == :draining
    end)

    :ok = Ops.undrain_node(local)

    eventually(fn ->
      assert remote_node_status(peer_a, local) == :ready
      assert remote_node_status(peer_b, local) == :ready
    end)
  end

  test "drain intent recovers after full peer node restart", %{
    peers: peers,
    write_nodes: write_nodes
  } do
    [target_peer, witness_peer] = peers
    {target_pid, target_node} = target_peer
    {_witness_pid, witness_node} = witness_peer
    local = Node.self()

    :ok = Ops.undrain_node(local)
    :ok = Ops.drain_node(local)

    eventually(fn ->
      assert remote_node_status(target_node, local) == :draining
      assert remote_node_status(witness_node, local) == :draining
    end)

    :ok = stop_peer_observer(target_node)
    :ok = stop_peer(target_pid)

    eventually(fn ->
      assert Node.ping(target_node) == :pang
    end)

    restarted_peer = start_named_peer(node_name(target_node))
    {restarted_pid, restarted_node} = restarted_peer
    assert restarted_node == target_node

    on_exit(fn ->
      _ = stop_peer_observer(restarted_node)
      _ = stop_peer(restarted_pid)
    end)

    true = Node.connect(restarted_node)
    :ok = add_code_paths(restarted_node)
    :ok = configure_peer(restarted_node, write_nodes)
    :ok = start_peer_observer(restarted_node)

    eventually(fn ->
      assert remote_node_status(restarted_node, local) == :draining
    end)

    :ok = Ops.undrain_node(local)

    eventually(fn ->
      assert remote_node_status(restarted_node, local) == :ready
      assert remote_node_status(witness_node, local) == :ready
    end)
  end

  defp start_peers(count) when is_integer(count) and count > 0 do
    Enum.map(1..count, fn _index ->
      start_named_peer(:"observer_peer_#{System.unique_integer([:positive])}")
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

  defp configure_peer(peer_node, write_nodes)
       when is_atom(peer_node) and is_list(write_nodes) do
    :ok = :rpc.call(peer_node, Application, :put_env, [:starcite, :write_node_ids, write_nodes])
    :ok = :rpc.call(peer_node, Application, :put_env, [:starcite, :write_replication_factor, 1])
    :ok
  end

  defp start_peer_observer(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, GenServer, :start, [Observer, [], [name: Observer]]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      other -> raise "failed to start observer on #{peer_node}: #{inspect(other)}"
    end
  end

  defp stop_peer_observer(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, Process, :whereis, [Observer]) do
      pid when is_pid(pid) ->
        case :rpc.call(peer_node, GenServer, :stop, [Observer, :normal, 5_000]) do
          :ok -> :ok
          {:badrpc, _reason} -> :ok
          _other -> :ok
        end

      nil ->
        :ok

      {:badrpc, _reason} ->
        :ok
    end
  end

  defp stop_peer(peer_pid) when is_pid(peer_pid) do
    :peer.stop(peer_pid)
  catch
    :exit, _reason -> :ok
  end

  defp node_name(node) when is_atom(node) do
    node
    |> Atom.to_string()
    |> String.split("@", parts: 2)
    |> hd()
    |> String.to_atom()
  end

  defp remote_node_status(peer_node, node) when is_atom(peer_node) and is_atom(node) do
    case :rpc.call(peer_node, Observer, :status, [], 5_000) do
      %{status: :ok, node_statuses: %{^node => %{status: status}}} ->
        status

      other ->
        raise "unexpected observer status from #{peer_node}: #{inspect(other)}"
    end
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
