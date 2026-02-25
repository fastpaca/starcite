defmodule StarciteWeb.OpsControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.ControlPlane.{Observer, Ops}

  @endpoint StarciteWeb.Endpoint

  setup do
    Starcite.Runtime.TestHelper.reset()
    :ok = Ops.undrain_node(Node.self())

    on_exit(fn ->
      :ok = Ops.undrain_node(Node.self())
    end)

    :ok
  end

  defp json_conn(method, path, body) do
    conn =
      conn(method, path)
      |> put_req_header("content-type", "application/json")

    conn =
      if body do
        %{conn | body_params: body, params: body}
      else
        conn
      end

    @endpoint.call(conn, @endpoint.init([]))
  end

  describe "GET /v1/ops/status" do
    test "returns write-node and observer diagnostics" do
      conn = json_conn(:get, "/v1/ops/status", nil)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)

      assert body["node"] == Atom.to_string(Node.self())
      assert is_boolean(body["local_write_node"])
      assert body["local_mode"] in ["write_node", "router_node"]
      assert is_boolean(body["local_ready"])
      assert is_boolean(body["local_drained"])
      assert is_list(body["write_nodes"])
      assert is_integer(body["write_replication_factor"])
      assert is_integer(body["num_groups"])
      assert is_list(body["local_groups"])
      assert is_map(body["raft_storage"])
      assert is_binary(body["raft_storage"]["starcite_data_dir"])
      assert is_binary(body["raft_storage"]["ra_data_dir"])
      assert is_binary(body["raft_storage"]["ra_wal_data_dir"])
      assert is_map(body["observer"])
      assert body["observer"]["status"] in ["ok", "down"]
      assert is_list(body["observer"]["visible_nodes"])
      assert is_list(body["observer"]["ready_nodes"])
      assert is_map(body["observer"]["node_statuses"])
    end
  end

  describe "GET /v1/ops/ready-nodes" do
    test "returns routing-eligible ready nodes" do
      conn = json_conn(:get, "/v1/ops/ready-nodes", nil)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert Atom.to_string(Node.self()) in body["ready_nodes"]
    end
  end

  describe "POST /v1/ops/drain and /v1/ops/undrain" do
    test "drains and restores local node routing eligibility" do
      local_node = Node.self()
      local_name = Atom.to_string(local_node)
      assert local_node in Observer.ready_nodes()

      drain_conn = json_conn(:post, "/v1/ops/drain", %{})

      assert drain_conn.status == 200
      assert Jason.decode!(drain_conn.resp_body) == %{"status" => "ok", "node" => local_name}

      eventually(fn ->
        refute local_node in Observer.ready_nodes()
      end)

      undrain_conn = json_conn(:post, "/v1/ops/undrain", %{"node" => local_name})

      assert undrain_conn.status == 200

      eventually(fn ->
        assert local_node in Observer.ready_nodes()
      end)
    end

    test "returns 400 for unknown write node" do
      conn = json_conn(:post, "/v1/ops/drain", %{"node" => "missing@starcite.internal"})

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_write_node"
      assert is_binary(body["message"])
    end
  end

  describe "GET /v1/ops/groups/:group_id/replicas" do
    test "returns configured replicas for a write group" do
      group_id = min(42, Starcite.ControlPlane.WriteNodes.num_groups() - 1)
      conn = json_conn(:get, "/v1/ops/groups/#{group_id}/replicas", nil)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["group_id"] == group_id
      assert body["replicas"] == [Atom.to_string(Node.self())]
    end

    test "returns 400 for invalid group id" do
      conn = json_conn(:get, "/v1/ops/groups/not-a-number/replicas", nil)

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_group_id"
      assert is_binary(body["message"])
    end
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
