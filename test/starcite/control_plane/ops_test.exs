defmodule Starcite.ControlPlane.OpsTest do
  use ExUnit.Case, async: false

  alias Starcite.ControlPlane.{Observer, Ops}

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
