defmodule StarciteWeb.FallbackControllerTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias StarciteWeb.FallbackController

  test "maps replication quorum failure to service unavailable" do
    conn =
      conn(:get, "/")
      |> FallbackController.call(
        {:error,
         {:replication_quorum_not_met,
          %{
            required_remote_acks: 1,
            successful_remote_acks: 0,
            failures: [{:"missing@127.0.0.1", {:badrpc, :nodedown}}]
          }}}
      )

    assert conn.status == 503
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "replication_unavailable"
    assert body["message"] == "In-memory replication quorum was not met"
  end

  test "maps not_leader to service unavailable" do
    conn =
      conn(:get, "/")
      |> FallbackController.call({:error, :not_leader})

    assert conn.status == 503
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "owner_unavailable"
    assert body["message"] == "No active owner for session group"
  end

  test "maps not_leader with redirect hint to service unavailable" do
    conn =
      conn(:get, "/")
      |> FallbackController.call({:error, {:not_leader, {:raft_group_7, :"leader@127.0.0.1"}}})

    assert conn.status == 503
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "owner_unavailable"
    assert body["message"] == "No active owner for session group"
  end

  test "maps timeout to routing timeout error" do
    conn =
      conn(:get, "/")
      |> FallbackController.call({:timeout, {:raft_group_7, :"leader@127.0.0.1"}})

    assert conn.status == 503
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "routing_timeout"
    assert body["message"] == "Control-plane routing request timed out"
  end

  test "maps khepri mismatching_node to owner unavailable" do
    conn =
      conn(:get, "/")
      |> FallbackController.call(
        {:error,
         {:khepri, :mismatching_node, %{path: [:transfers, "ses-123"], node: :node2@localhost}}}
      )

    assert conn.status == 503
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "owner_unavailable"
    assert body["message"] == "No active owner for session group"
  end
end
