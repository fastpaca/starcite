defmodule Starcite.Routing.SessionRouterTest do
  use ExUnit.Case, async: true

  alias Starcite.Routing.SessionRouter

  def local_echo(value), do: {:local, value}

  test "ensure_local_owner/2 accepts the assigned owner node" do
    session_id = "session-router-local-owner"
    assignment = %{owner: Node.self(), epoch: 3}

    assert :ok =
             SessionRouter.ensure_local_owner(session_id,
               self: Node.self(),
               assignment: assignment
             )
  end

  test "ensure_local_owner/2 returns not_leader with redirect hint when local node is not owner" do
    attach_routing_fence_handler()
    session_id = "session-router-follower"
    owner = :"owner@127.0.0.1"
    assignment = %{owner: owner, epoch: 5}

    assert {:error, {:not_leader, {:session_owner, ^owner}}} =
             SessionRouter.ensure_local_owner(session_id,
               self: :"follower@127.0.0.1",
               assignment: assignment
             )

    assert_receive {:routing_fence_event, %{count: 1},
                    %{session_id: ^session_id, source: :session_router, reason: :not_leader}},
                   1_000
  end

  test "ensure_local_owner/2 returns not_leader when no assignment exists" do
    session_id = "session-router-no-leader"

    assert {:error, :not_leader} = SessionRouter.ensure_local_owner(session_id, self: Node.self())
  end

  test "ensure_local_owner/2 rejects moving assignments" do
    attach_routing_fence_handler()
    session_id = "session-router-moving"
    assignment = %{owner: Node.self(), epoch: 4, status: :moving}

    assert {:error, :ownership_transfer_in_progress} =
             SessionRouter.ensure_local_owner(session_id,
               self: Node.self(),
               assignment: assignment
             )

    assert_receive {:routing_fence_event, %{count: 1},
                    %{
                      session_id: ^session_id,
                      source: :session_router,
                      reason: :ownership_transfer_in_progress
                    }},
                   1_000
  end

  test "local_owner_epoch/3 uses assignment epoch when available" do
    assert 11 ==
             SessionRouter.local_owner_epoch(
               "session-router-epoch-term",
               0,
               assignment: %{owner: Node.self(), epoch: 11}
             )
  end

  test "local_owner_epoch/3 falls back when assignment omits epoch" do
    assert 7 ==
             SessionRouter.local_owner_epoch(
               "session-router-epoch-fallback",
               7,
               assignment: %{owner: Node.self()}
             )
  end

  test "local_owner_epoch/3 never regresses below fallback epoch" do
    assert 9 ==
             SessionRouter.local_owner_epoch(
               "session-router-epoch-monotonic",
               9,
               assignment: %{owner: Node.self(), epoch: 3}
             )
  end

  test "call/8 routes locally when the local node owns the session" do
    session_id = "session-router-call-local"

    assert {:local, :ok} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [:ignored],
               __MODULE__,
               :local_echo,
               [:ok],
               assignment: %{owner: Node.self(), epoch: 1}
             )
  end

  test "call/8 prefers the local follower when prefer_leader is false" do
    session_id = "session-router-local-follower"
    local_node = :local@cluster
    owner = :owner@cluster

    assert {:local, :ok} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [:ignored],
               __MODULE__,
               :local_echo,
               [:ok],
               assignment: %{owner: owner, epoch: 1, replicas: [owner, local_node]},
               prefer_leader: false,
               self: local_node
             )
  end

  test "call/8 prefers a remote follower before the owner when prefer_leader is false" do
    session_id = "session-router-remote-follower"
    local_node = :local@cluster
    owner = :owner@cluster
    follower = :follower@cluster

    rpc_fun = fn node, _module, _fun, _args ->
      send(self(), {:rpc_called, node})
      {:ok, {:routed, node}}
    end

    assert {:ok, {:routed, ^follower}} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: %{owner: owner, epoch: 1, replicas: [owner, follower]},
               prefer_leader: false,
               rpc_fun: rpc_fun,
               self: local_node
             )

    assert_receive {:rpc_called, ^follower}
    refute_receive {:rpc_called, ^owner}
  end

  test "call/8 falls back to the owner when follower replicas are unavailable" do
    session_id = "session-router-owner-fallback"
    local_node = :local@cluster
    owner = :owner@cluster
    follower = :follower@cluster

    rpc_fun = fn node, _module, _fun, _args ->
      send(self(), {:rpc_called, node})

      case node do
        ^follower -> {:badrpc, :nodedown}
        ^owner -> {:ok, {:routed, owner}}
      end
    end

    assert {:ok, {:routed, ^owner}} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: %{owner: owner, epoch: 1, replicas: [owner, follower]},
               prefer_leader: false,
               rpc_fun: rpc_fun,
               self: local_node
             )

    assert_receive {:rpc_called, ^follower}
    assert_receive {:rpc_called, ^owner}
  end

  test "call/8 refreshes the authoritative assignment on redirect before rerouting" do
    session_id = "session-router-refresh"
    original_owner = :"owner-a@cluster"
    refreshed_owner = :"owner-b@cluster"

    rpc_fun = fn owner, _module, _fun, _args ->
      send(self(), {:rpc_called, owner})

      if owner == original_owner do
        {:error, {:not_leader, {:session_owner, refreshed_owner}}}
      else
        {:ok, {:routed, owner}}
      end
    end

    assignment_fetcher = fn ^session_id ->
      {:ok, %{owner: refreshed_owner, epoch: 2, status: :active}}
    end

    assert {:ok, {:routed, ^refreshed_owner}} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: %{owner: original_owner, epoch: 1, status: :active},
               assignment_fetcher: assignment_fetcher,
               rpc_fun: rpc_fun,
               self: :local@cluster
             )

    assert_receive {:rpc_called, ^original_owner}
    assert_receive {:rpc_called, ^refreshed_owner}
  end

  test "call/8 rejects redirect hints when the authoritative assignment is unchanged" do
    session_id = "session-router-stale-redirect"
    original_owner = :"owner-a@cluster"
    redirect_owner = :"owner-b@cluster"

    rpc_fun = fn owner, _module, _fun, _args ->
      send(self(), {:rpc_called, owner})
      {:error, {:not_leader, {:session_owner, redirect_owner}}}
    end

    assignment_fetcher = fn ^session_id ->
      {:ok, %{owner: original_owner, epoch: 1, status: :active}}
    end

    assert {:error, {:routing_rpc_failed, ^original_owner, :stale_assignment}} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: %{owner: original_owner, epoch: 1, status: :active},
               assignment_fetcher: assignment_fetcher,
               rpc_fun: rpc_fun,
               self: :local@cluster
             )

    assert_receive {:rpc_called, ^original_owner}
    refute_receive {:rpc_called, ^redirect_owner}
  end

  test "call/8 surfaces ownership transfer in progress after an authoritative refresh" do
    session_id = "session-router-moving-refresh"
    original_owner = :"owner-a@cluster"
    redirect_owner = :"owner-b@cluster"

    rpc_fun = fn owner, _module, _fun, _args ->
      send(self(), {:rpc_called, owner})
      {:error, {:not_leader, {:session_owner, redirect_owner}}}
    end

    assignment_fetcher = fn ^session_id ->
      {:ok,
       %{
         owner: original_owner,
         epoch: 2,
         status: :moving,
         target_owner: redirect_owner,
         transfer_id: "xfer-1"
       }}
    end

    assert {:error, :ownership_transfer_in_progress} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: %{owner: original_owner, epoch: 1, status: :active},
               assignment_fetcher: assignment_fetcher,
               rpc_fun: rpc_fun,
               self: :local@cluster
             )

    assert_receive {:rpc_called, ^original_owner}
    refute_receive {:rpc_called, ^redirect_owner}
  end

  test "call/8 emits routing telemetry for authoritative refresh success" do
    attach_routing_handler()
    session_id = "session-router-telemetry-refresh"
    original_owner = :"owner-a@cluster"
    refreshed_owner = :"owner-b@cluster"

    rpc_fun = fn owner, _module, _fun, _args ->
      if owner == original_owner do
        {:error, {:not_leader, {:session_owner, refreshed_owner}}}
      else
        {:ok, {:routed, owner}}
      end
    end

    assignment_fetcher = fn ^session_id ->
      {:ok, %{owner: refreshed_owner, epoch: 2, status: :active}}
    end

    assert {:ok, {:routed, ^refreshed_owner}} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: %{owner: original_owner, epoch: 1, status: :active},
               assignment_fetcher: assignment_fetcher,
               rpc_fun: rpc_fun,
               self: :local@cluster
             )

    assert_receive {:routing_result_event, %{count: 1, refreshes: 1},
                    %{target: :remote, outcome: :ok, error_reason: :none}},
                   1_000
  end

  test "call/8 emits routing telemetry for stale assignment failure" do
    attach_routing_handler()
    session_id = "session-router-telemetry-stale"
    original_owner = :"owner-a@cluster"
    redirect_owner = :"owner-b@cluster"

    rpc_fun = fn _owner, _module, _fun, _args ->
      {:error, {:not_leader, {:session_owner, redirect_owner}}}
    end

    assignment_fetcher = fn ^session_id ->
      {:ok, %{owner: original_owner, epoch: 1, status: :active}}
    end

    assert {:error, {:routing_rpc_failed, ^original_owner, :stale_assignment}} =
             SessionRouter.call(
               session_id,
               __MODULE__,
               :remote_echo,
               [],
               __MODULE__,
               :local_echo,
               [],
               assignment: %{owner: original_owner, epoch: 1, status: :active},
               assignment_fetcher: assignment_fetcher,
               rpc_fun: rpc_fun,
               self: :local@cluster
             )

    assert_receive {:routing_result_event, %{count: 1, refreshes: 1},
                    %{target: :remote, outcome: :error, error_reason: :stale_assignment}},
                   1_000
  end

  defp attach_routing_handler do
    handler_id = "routing-result-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :routing, :result],
        fn _event, measurements, metadata, pid ->
          send(pid, {:routing_result_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp attach_routing_fence_handler do
    handler_id = "routing-fence-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :routing, :fence],
        fn _event, measurements, metadata, pid ->
          send(pid, {:routing_fence_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end
end
