defmodule Starcite.Routing.SessionRouterTest do
  use ExUnit.Case, async: true

  alias Starcite.Routing.SessionRouter

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
    session_id = "session-router-follower"
    owner = :"owner@127.0.0.1"
    assignment = %{owner: owner, epoch: 5}

    assert {:error, {:not_leader, {:session_owner, ^owner}}} =
             SessionRouter.ensure_local_owner(session_id,
               self: :"follower@127.0.0.1",
               assignment: assignment
             )
  end

  test "ensure_local_owner/2 returns not_leader when no assignment exists" do
    session_id = "session-router-no-leader"

    assert {:error, :not_leader} = SessionRouter.ensure_local_owner(session_id, self: Node.self())
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
end
