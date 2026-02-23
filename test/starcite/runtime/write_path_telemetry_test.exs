defmodule Starcite.Runtime.WritePathTelemetryTest do
  use ExUnit.Case, async: false

  alias Starcite.Observability.Telemetry
  alias Starcite.WritePath

  setup do
    Starcite.Runtime.TestHelper.reset()

    handler_id = "raft-command-#{System.unique_integer([:positive, :monotonic])}"
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

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    :ok
  end

  test "append emits local_ok command telemetry" do
    id = unique_id("ses")
    assert {:ok, _session} = WritePath.create_session(id: id)

    assert {:ok, _reply} =
             WritePath.append_event(id, %{
               type: "content",
               payload: %{text: "hello"},
               actor: "agent:1",
               producer_id: "writer:test",
               producer_seq: 1
             })

    assert_receive_raft_command(:append_event, :local_ok)
  end

  test "append missing session emits local_error command telemetry" do
    id = unique_id("missing")

    assert {:error, :session_not_found} =
             WritePath.append_event(id, %{
               type: "content",
               payload: %{text: "hello"},
               actor: "agent:1",
               producer_id: "writer:test",
               producer_seq: 1
             })

    assert_receive_raft_command(:append_event, :local_error)
  end

  test "telemetry helper exposes leader_retry outcome dimension" do
    assert :ok = Telemetry.raft_command_result(:append_event, :leader_retry_timeout)

    assert_receive_raft_command(:append_event, :leader_retry_timeout)
  end

  defp assert_receive_raft_command(command, outcome) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_raft_command(command, outcome, deadline)
  end

  defp do_assert_receive_raft_command(command, outcome, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:raft_command_event, %{count: 1}, %{command: ^command, outcome: ^outcome}} ->
        :ok

      {:raft_command_event, _measurements, _metadata} ->
        do_assert_receive_raft_command(command, outcome, deadline)
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
