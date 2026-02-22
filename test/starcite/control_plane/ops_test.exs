defmodule Starcite.ControlPlane.OpsTest do
  use ExUnit.Case, async: false

  alias Starcite.ControlPlane.{Observer, Ops, WriteNodes}

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
