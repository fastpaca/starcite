defmodule Starcite.Runtime.WritePathTelemetryTest do
  use ExUnit.Case, async: false

  alias Starcite.Observability.Telemetry
  alias Starcite.WritePath

  setup do
    Starcite.Runtime.TestHelper.reset()

    handler_id = "raft-command-#{System.unique_integer([:positive, :monotonic])}"
    role_count_handler_id = "raft-role-count-#{System.unique_integer([:positive, :monotonic])}"
    group_role_handler_id = "raft-group-role-#{System.unique_integer([:positive, :monotonic])}"

    leadership_transfer_handler_id =
      "raft-leadership-transfer-#{System.unique_integer([:positive, :monotonic])}"

    request_handler_id = "write-request-#{System.unique_integer([:positive, :monotonic])}"
    session_handler_id = "session-lifecycle-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :raft, :command],
        fn _event, measurements, metadata, pid ->
          send(pid, {:raft_command_event, measurements, metadata})
        end,
        test_pid
      )

    :ok =
      :telemetry.attach(
        role_count_handler_id,
        [:starcite, :raft, :role_count],
        fn _event, measurements, metadata, pid ->
          send(pid, {:raft_role_count_event, measurements, metadata})
        end,
        test_pid
      )

    :ok =
      :telemetry.attach(
        group_role_handler_id,
        [:starcite, :raft, :group_role],
        fn _event, measurements, metadata, pid ->
          send(pid, {:raft_group_role_event, measurements, metadata})
        end,
        test_pid
      )

    :ok =
      :telemetry.attach(
        leadership_transfer_handler_id,
        [:starcite, :raft, :leadership_transfer],
        fn _event, measurements, metadata, pid ->
          send(pid, {:raft_leadership_transfer_event, measurements, metadata})
        end,
        test_pid
      )

    :ok =
      :telemetry.attach(
        request_handler_id,
        [:starcite, :request],
        fn _event, measurements, metadata, pid ->
          send(pid, {:request_event, measurements, metadata})
        end,
        test_pid
      )

    :ok =
      :telemetry.attach_many(
        session_handler_id,
        [
          [:starcite, :session, :create],
          [:starcite, :session, :freeze],
          [:starcite, :session, :hydrate]
        ],
        fn event_name, measurements, metadata, pid ->
          send(pid, {:session_event, event_name, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      :telemetry.detach(role_count_handler_id)
      :telemetry.detach(group_role_handler_id)
      :telemetry.detach(leadership_transfer_handler_id)
      :telemetry.detach(request_handler_id)
      :telemetry.detach(session_handler_id)
    end)

    :ok
  end

  test "append does not emit per-command telemetry from write path" do
    id = unique_id("ses")
    assert {:ok, _session} = WritePath.create_session(id: id, tenant_id: "acme")

    assert {:ok, _reply} =
             WritePath.append_event(id, %{
               type: "content",
               payload: %{text: "hello"},
               actor: "agent:1",
               metadata: %{"tenant_id" => "acme"},
               producer_id: "writer:test",
               producer_seq: 1
             })

    refute_receive {:raft_command_event, _measurements, _metadata}, 100
  end

  test "append does not emit write request telemetry from write path" do
    id = unique_id("ses")
    assert {:ok, _session} = WritePath.create_session(id: id, tenant_id: "acme")

    assert {:ok, _reply} =
             WritePath.append_event(id, %{
               type: "content",
               payload: %{text: "hello"},
               actor: "agent:1",
               metadata: %{"tenant_id" => "acme"},
               producer_id: "writer:test",
               producer_seq: 1
             })

    refute_receive {:request_event, _measurements, _metadata}, 100
  end

  test "append missing session does not emit per-command telemetry from write path" do
    id = unique_id("missing")

    assert {:error, :session_not_found} =
             WritePath.append_event(id, %{
               type: "content",
               payload: %{text: "hello"},
               actor: "agent:1",
               metadata: %{"tenant_id" => "acme"},
               producer_id: "writer:test",
               producer_seq: 1
             })

    refute_receive {:raft_command_event, _measurements, _metadata}, 100
  end

  test "append missing session does not emit write request telemetry from write path" do
    id = unique_id("missing")

    assert {:error, :session_not_found} =
             WritePath.append_event(id, %{
               type: "content",
               payload: %{text: "hello"},
               actor: "agent:1",
               metadata: %{"tenant_id" => "acme"},
               producer_id: "writer:test",
               producer_seq: 1
             })

    refute_receive {:request_event, _measurements, _metadata}, 100
  end

  test "append_events does not emit write request telemetry from write path" do
    id = unique_id("ses")
    assert {:ok, _session} = WritePath.create_session(id: id, tenant_id: "acme")

    events = [
      %{
        type: "content",
        payload: %{text: "hello"},
        actor: "agent:1",
        metadata: %{"tenant_id" => "acme"},
        producer_id: "writer:test",
        producer_seq: 1
      },
      %{
        type: "content",
        payload: %{text: "world"},
        actor: "agent:1",
        metadata: %{"tenant_id" => "acme"},
        producer_id: "writer:test",
        producer_seq: 2
      }
    ]

    assert {:ok, _reply} = WritePath.append_events(id, events)
    refute_receive {:request_event, _measurements, _metadata}, 100
  end

  test "telemetry helper exposes leader_retry outcome dimension" do
    assert :ok = Telemetry.raft_command_result(:append_event, :leader_retry_timeout, "acme")

    assert_receive_raft_command(:append_event, :leader_retry_timeout, "acme")
  end

  test "telemetry helper exposes raft group role count dimensions" do
    assert :ok = Telemetry.raft_group_role_count("shared-1@127.0.0.1", :leader, 32)

    assert_receive {:raft_role_count_event, %{groups: 32},
                    %{node: "shared-1@127.0.0.1", role: :leader}},
                   1_000
  end

  test "telemetry helper exposes per-group raft role dimensions" do
    assert :ok = Telemetry.raft_group_role_presence("shared-1@127.0.0.1", 7, :follower, 1)

    assert_receive {:raft_group_role_event, %{present: 1},
                    %{node: "shared-1@127.0.0.1", group_id: 7, role: :follower}},
                   1_000
  end

  test "telemetry helper exposes leadership transfer dimensions" do
    assert :ok =
             Telemetry.raft_leadership_transfer(
               7,
               "shared-0@127.0.0.1",
               "shared-1@127.0.0.1",
               :timeout,
               :timeout
             )

    assert_receive {:raft_leadership_transfer_event, %{count: 1},
                    %{
                      group_id: 7,
                      source_node: "shared-0@127.0.0.1",
                      target_node: "shared-1@127.0.0.1",
                      outcome: :timeout,
                      reason: :timeout
                    }},
                   1_000
  end

  test "create_session emits session lifecycle telemetry" do
    id = unique_id("ses")
    assert {:ok, _session} = WritePath.create_session(id: id, tenant_id: "acme")

    assert_receive {:session_event, [:starcite, :session, :create], %{count: 1},
                    %{session_id: ^id, tenant_id: "acme"}},
                   1_000
  end

  test "telemetry helper exposes freeze and hydrate dimensions" do
    assert :ok = Telemetry.session_freeze("ses-1", "acme", :conflict, :freeze_conflict)

    assert_receive {:session_event, [:starcite, :session, :freeze], %{count: 1},
                    %{
                      session_id: "ses-1",
                      tenant_id: "acme",
                      outcome: :conflict,
                      reason: :freeze_conflict
                    }},
                   1_000

    assert :ok = Telemetry.session_hydrate("ses-1", "acme", :ok, :hydrated)

    assert_receive {:session_event, [:starcite, :session, :hydrate], %{count: 1},
                    %{session_id: "ses-1", tenant_id: "acme", outcome: :ok, reason: :hydrated}},
                   1_000
  end

  test "telemetry helper exposes write request dimensions" do
    assert :ok = Telemetry.request(:append_event, :ack, :timeout, 7)

    assert_receive {:request_event, %{count: 1, duration_ms: 7},
                    %{operation: :append_event, phase: :ack, outcome: :timeout}},
                   1_000
  end

  test "global telemetry flag disables raft command events" do
    original = Application.get_env(:starcite, :telemetry_enabled, false)
    Application.put_env(:starcite, :telemetry_enabled, false)

    on_exit(fn ->
      Application.put_env(:starcite, :telemetry_enabled, original)
    end)

    assert :ok = Telemetry.raft_command_result(:append_event, :leader_retry_timeout, "acme")
    refute_receive {:raft_command_event, _measurements, _metadata}, 100
  end

  test "global telemetry flag disables write request helper events" do
    original = Application.get_env(:starcite, :telemetry_enabled, false)
    Application.put_env(:starcite, :telemetry_enabled, false)

    on_exit(fn ->
      Application.put_env(:starcite, :telemetry_enabled, original)
    end)

    assert :ok = Telemetry.request(:append_event, :ack, :ok, 5)

    refute_receive {:request_event, _measurements, _metadata}, 100
  end

  defp assert_receive_raft_command(command, outcome, tenant_id) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_raft_command(command, outcome, tenant_id, deadline)
  end

  defp do_assert_receive_raft_command(command, outcome, tenant_id, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:raft_command_event, %{count: 1},
       %{command: ^command, outcome: ^outcome, tenant_id: ^tenant_id}} ->
        :ok

      {:raft_command_event, _measurements, _metadata} ->
        do_assert_receive_raft_command(command, outcome, tenant_id, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for raft command telemetry command=#{inspect(command)} outcome=#{inspect(outcome)}"
        )
    end
  end

  defp unique_id(prefix) when is_binary(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end
end
