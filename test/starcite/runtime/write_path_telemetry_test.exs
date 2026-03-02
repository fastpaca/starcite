defmodule Starcite.Runtime.WritePathTelemetryTest do
  use ExUnit.Case, async: false

  alias Starcite.Observability.Telemetry
  alias Starcite.WritePath

  setup do
    Starcite.Runtime.TestHelper.reset()

    handler_id = "raft-command-#{System.unique_integer([:positive, :monotonic])}"
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
          [:starcite, :session, :eviction_tick],
          [:starcite, :session, :freeze, :success],
          [:starcite, :session, :freeze, :conflict],
          [:starcite, :session, :freeze, :error],
          [:starcite, :session, :hydrate, :attempt],
          [:starcite, :session, :hydrate, :success],
          [:starcite, :session, :hydrate, :error]
        ],
        fn event_name, measurements, metadata, pid ->
          send(pid, {:session_event, event_name, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
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

    assert {:error, reason} =
             WritePath.append_event(id, %{
               type: "content",
               payload: %{text: "hello"},
               actor: "agent:1",
               metadata: %{"tenant_id" => "acme"},
               producer_id: "writer:test",
               producer_seq: 1
             })

    assert reason in [:session_not_found, :archive_read_unavailable]

    refute_receive {:raft_command_event, _measurements, _metadata}, 100
  end

  test "append missing session does not emit write request telemetry from write path" do
    id = unique_id("missing")

    assert {:error, reason} =
             WritePath.append_event(id, %{
               type: "content",
               payload: %{text: "hello"},
               actor: "agent:1",
               metadata: %{"tenant_id" => "acme"},
               producer_id: "writer:test",
               producer_seq: 1
             })

    assert reason in [:session_not_found, :archive_read_unavailable]

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

  test "telemetry helper exposes session eviction tick dimensions" do
    assert :ok = Telemetry.session_eviction_tick(7, 3, 2, 11, 4, 10)

    assert_receive {:session_event, [:starcite, :session, :eviction_tick],
                    %{count: 1, candidates: 2, hot_sessions: 11},
                    %{
                      group_id: 7,
                      poll_epoch: 3,
                      min_idle_polls: 4,
                      max_freeze_batch_size: 10
                    }},
                   1_000
  end

  test "telemetry helper exposes session freeze outcome dimensions" do
    assert :ok = Telemetry.session_freeze_success("ses-a", "acme")
    assert :ok = Telemetry.session_freeze_conflict("ses-a", "acme")
    assert :ok = Telemetry.session_freeze_error("ses-a", "acme", :archive_write_unavailable)

    assert_receive {:session_event, [:starcite, :session, :freeze, :success], %{count: 1},
                    %{session_id: "ses-a", tenant_id: "acme"}},
                   1_000

    assert_receive {:session_event, [:starcite, :session, :freeze, :conflict], %{count: 1},
                    %{session_id: "ses-a", tenant_id: "acme"}},
                   1_000

    assert_receive {:session_event, [:starcite, :session, :freeze, :error], %{count: 1},
                    %{
                      session_id: "ses-a",
                      tenant_id: "acme",
                      reason: :archive_write_unavailable
                    }},
                   1_000
  end

  test "telemetry helper exposes session hydrate dimensions" do
    assert :ok = Telemetry.session_hydrate_attempt("ses-hydrate")
    assert :ok = Telemetry.session_hydrate_success("ses-hydrate", :hydrated)
    assert :ok = Telemetry.session_hydrate_error("ses-hydrate", :archive_read_unavailable)

    assert_receive {:session_event, [:starcite, :session, :hydrate, :attempt], %{count: 1},
                    %{session_id: "ses-hydrate"}},
                   1_000

    assert_receive {:session_event, [:starcite, :session, :hydrate, :success], %{count: 1},
                    %{session_id: "ses-hydrate", result: :hydrated}},
                   1_000

    assert_receive {:session_event, [:starcite, :session, :hydrate, :error], %{count: 1},
                    %{session_id: "ses-hydrate", reason: :archive_read_unavailable}},
                   1_000
  end

  test "global telemetry flag disables session lifecycle helper events" do
    original = Application.get_env(:starcite, :telemetry_enabled, false)
    Application.put_env(:starcite, :telemetry_enabled, false)

    on_exit(fn ->
      Application.put_env(:starcite, :telemetry_enabled, original)
    end)

    assert :ok = Telemetry.session_eviction_tick(0, 1, 0, 0, 1, 1)
    assert :ok = Telemetry.session_freeze_success("ses-a", "acme")
    assert :ok = Telemetry.session_hydrate_attempt("ses-a")

    refute_receive {:session_event, _event_name, _measurements, _metadata}, 100
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
