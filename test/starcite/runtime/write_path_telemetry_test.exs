defmodule Starcite.Runtime.WritePathTelemetryTest do
  use ExUnit.Case, async: false

  alias Starcite.Observability.Telemetry
  alias Starcite.WritePath

  setup do
    Starcite.Runtime.TestHelper.reset()

    handler_id = "raft-command-#{System.unique_integer([:positive, :monotonic])}"
    role_count_handler_id = "raft-role-count-#{System.unique_integer([:positive, :monotonic])}"
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
      :telemetry.detach(request_handler_id)
      :telemetry.detach(session_handler_id)
    end)

    :ok
  end

  test "append emits per-command telemetry from write path" do
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

    assert_receive_raft_command(:append_event, :local_ok)
  end

  test "append emits write request telemetry from write path" do
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

    assert_receive_request_event(:append_event, :ack, :ok)
    assert_receive_request_event(:append_event, :route, :ok)
  end

  test "append missing session emits per-command telemetry from write path" do
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

    assert_receive_raft_command(:append_event, :local_error)
  end

  test "append missing session emits write request telemetry from write path" do
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

    assert_receive_request_event(:append_event, :ack, :error)
    assert_receive_request_event(:append_event, :route, :error)
  end

  test "append_events emits write request telemetry from write path" do
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
    assert_receive_raft_command(:append_events, :local_ok)
    assert_receive_request_event(:append_events, :ack, :ok)
    assert_receive_request_event(:append_events, :route, :ok)
  end

  test "telemetry helper exposes leader_retry outcome dimension" do
    node_name = "shared-1@127.0.0.1"
    assert :ok = Telemetry.raft_command_result(:append_event, :leader_retry_timeout, node_name)

    assert_receive_raft_command(:append_event, :leader_retry_timeout, node_name)
  end

  test "telemetry helper emits command attempt telemetry for append acknowledgements" do
    node_name = "shared-1@127.0.0.1"

    assert :ok =
             Telemetry.raft_command_attempt(
               :append_event,
               :leader_retry_ok,
               node_name,
               :append_event,
               {:ok, %{seq: 1}},
               9
             )

    assert_receive_raft_command(:append_event, :leader_retry_ok, node_name)

    assert_receive {:request_event, %{count: 1, duration_ms: 9},
                    %{node: ^node_name, operation: :append_event, phase: :ack, outcome: :ok}},
                   1_000
  end

  test "telemetry helper exposes raft group role count dimensions" do
    assert :ok = Telemetry.raft_group_role_count("shared-1@127.0.0.1", :leader, 32)

    assert_receive {:raft_role_count_event, %{groups: 32},
                    %{node: "shared-1@127.0.0.1", role: :leader}},
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
    node_name = "shared-1@127.0.0.1"
    assert :ok = Telemetry.request(:append_event, :ack, :timeout, 7, node_name)

    assert_receive {:request_event, %{count: 1, duration_ms: 7},
                    %{node: ^node_name, operation: :append_event, phase: :ack, outcome: :timeout}},
                   1_000
  end

  test "global telemetry flag disables raft command events" do
    original = Application.get_env(:starcite, :telemetry_enabled, false)
    Application.put_env(:starcite, :telemetry_enabled, false)

    on_exit(fn ->
      Application.put_env(:starcite, :telemetry_enabled, original)
    end)

    assert :ok =
             Telemetry.raft_command_result(
               :append_event,
               :leader_retry_timeout,
               "shared-1@127.0.0.1"
             )

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

  defp assert_receive_raft_command(command, outcome, node_name \\ Atom.to_string(Node.self())) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_raft_command(command, outcome, node_name, deadline)
  end

  defp do_assert_receive_raft_command(command, outcome, node_name, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:raft_command_event, %{count: 1},
       %{node: ^node_name, command: ^command, outcome: ^outcome}} ->
        :ok

      {:raft_command_event, _measurements, _metadata} ->
        do_assert_receive_raft_command(command, outcome, node_name, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for raft command telemetry command=#{inspect(command)} outcome=#{inspect(outcome)}"
        )
    end
  end

  defp assert_receive_request_event(operation, phase, outcome) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_request_event(operation, phase, outcome, deadline)
  end

  defp do_assert_receive_request_event(operation, phase, outcome, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)
    node_name = Atom.to_string(Node.self())

    receive do
      {:request_event, %{count: 1, duration_ms: duration_ms},
       %{node: ^node_name, operation: ^operation, phase: ^phase, outcome: ^outcome}}
      when is_integer(duration_ms) and duration_ms >= 0 ->
        :ok

      {:request_event, _measurements, _metadata} ->
        do_assert_receive_request_event(operation, phase, outcome, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for request telemetry operation=#{inspect(operation)} phase=#{inspect(phase)} outcome=#{inspect(outcome)}"
        )
    end
  end

  defp unique_id(prefix) when is_binary(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end
end
