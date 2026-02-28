defmodule Starcite.Runtime.WritePathTelemetryTest do
  use ExUnit.Case, async: false

  alias Starcite.Observability.Telemetry
  alias Starcite.WritePath

  setup do
    Starcite.Runtime.TestHelper.reset()

    handler_id = "raft-command-#{System.unique_integer([:positive, :monotonic])}"
    request_handler_id = "write-request-#{System.unique_integer([:positive, :monotonic])}"
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
        request_handler_id,
        [:starcite, :request],
        fn _event, measurements, metadata, pid ->
          send(pid, {:request_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      :telemetry.detach(request_handler_id)
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

  test "append emits write request telemetry with ack phase and ok outcome" do
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

    assert_receive {:request_event, %{count: 1, duration_ms: duration_ms},
                    %{operation: :append_event, phase: :ack, outcome: :ok}},
                   1_000

    assert is_integer(duration_ms) and duration_ms >= 0
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

  test "append missing session emits write request telemetry with error outcome" do
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

    assert_receive {:request_event, %{count: 1, duration_ms: duration_ms},
                    %{operation: :append_event, phase: :ack, outcome: :error}},
                   1_000

    assert is_integer(duration_ms) and duration_ms >= 0
  end

  test "append_events emits write request telemetry with append_events operation" do
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

    assert_receive {:request_event, %{count: 1, duration_ms: duration_ms},
                    %{operation: :append_events, phase: :ack, outcome: :ok}},
                   1_000

    assert is_integer(duration_ms) and duration_ms >= 0
  end

  test "telemetry helper exposes leader_retry outcome dimension" do
    assert :ok = Telemetry.raft_command_result(:append_event, :leader_retry_timeout, "acme")

    assert_receive_raft_command(:append_event, :leader_retry_timeout, "acme")
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

  test "global telemetry flag disables write request events" do
    original = Application.get_env(:starcite, :telemetry_enabled, false)
    Application.put_env(:starcite, :telemetry_enabled, false)

    on_exit(fn ->
      Application.put_env(:starcite, :telemetry_enabled, original)
    end)

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
