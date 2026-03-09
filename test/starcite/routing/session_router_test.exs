defmodule Starcite.Routing.SessionRouterTest do
  use ExUnit.Case, async: true

  alias Starcite.Routing.SessionRouter
  alias Starcite.Routing.LeaseManager

  test "ensure_local_owner/2 accepts the active leader node" do
    session_id = "session-router-local-owner"
    group_id = LeaseManager.group_for_session(session_id)
    server_id = LeaseManager.server_id(group_id)
    leader_node = :"leader@127.0.0.1"

    assert :ok =
             SessionRouter.ensure_local_owner(session_id,
               self: leader_node,
               leader_hint: {server_id, leader_node}
             )
  end

  test "ensure_local_owner/2 returns not_leader with redirect hint when node is follower" do
    session_id = "session-router-follower"
    group_id = LeaseManager.group_for_session(session_id)
    server_id = LeaseManager.server_id(group_id)
    leader_node = :"leader@127.0.0.1"

    assert {:error, {:not_leader, {^server_id, ^leader_node}}} =
             SessionRouter.ensure_local_owner(session_id,
               self: :"follower@127.0.0.1",
               leader_hint: {server_id, leader_node}
             )
  end

  test "ensure_local_owner/2 returns not_leader when no leader is known" do
    session_id = "session-router-no-leader"

    assert {:error, :not_leader} =
             SessionRouter.ensure_local_owner(session_id,
               self: :"any@127.0.0.1",
               leader_hint: nil
             )
  end

  test "ensure_local_owner/2 allows singleton local replica when leader hint is unavailable" do
    session_id = "session-router-singleton-local"

    assert :ok =
             SessionRouter.ensure_local_owner(session_id,
               self: Node.self(),
               leader_hint: nil
             )
  end

  test "local_owner_epoch/3 uses raft term from metrics when available" do
    assert 11 ==
             SessionRouter.local_owner_epoch(
               "session-router-epoch-term",
               0,
               metrics: %{term: 11}
             )
  end

  test "local_owner_epoch/3 falls back when metrics omit term" do
    assert 7 ==
             SessionRouter.local_owner_epoch(
               "session-router-epoch-fallback",
               7,
               metrics: %{state: :unknown}
             )
  end

  test "local_owner_epoch/3 never regresses below fallback epoch" do
    assert 9 ==
             SessionRouter.local_owner_epoch(
               "session-router-epoch-monotonic",
               9,
               metrics: %{term: 3}
             )
  end
end
