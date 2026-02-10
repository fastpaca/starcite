defmodule StarciteWeb.TailSocketETSTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime
  alias StarciteWeb.TailSocket

  setup do
    Starcite.Runtime.TestHelper.reset()

    old_payload_plane = Application.get_env(:starcite, :payload_plane, :legacy)
    old_tail_source = Application.get_env(:starcite, :tail_source, :legacy)

    Application.put_env(:starcite, :payload_plane, :dual_write)
    Application.put_env(:starcite, :tail_source, :ets)

    on_exit(fn ->
      Application.put_env(:starcite, :payload_plane, old_payload_plane)
      Application.put_env(:starcite, :tail_source, old_tail_source)
    end)

    :ok
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp drain_until_idle(state, frames \\ [], remaining \\ 30)

  defp drain_until_idle(state, frames, 0), do: {Enum.reverse(frames), state}

  defp drain_until_idle(state, frames, remaining) do
    case TailSocket.handle_info(:drain_replay, state) do
      {:push, {:text, payload}, next_state} ->
        seq = payload |> Jason.decode!() |> Map.fetch!("seq")
        drain_until_idle(next_state, [seq | frames], remaining - 1)

      {:ok, next_state} ->
        idle? =
          next_state.replay_done and :queue.is_empty(next_state.replay_queue) and
            map_size(next_state.live_buffer) == 0 and not next_state.drain_scheduled

        if idle? do
          {Enum.reverse(frames), next_state}
        else
          drain_until_idle(next_state, frames, remaining - 1)
        end

      {:stop, _reason, next_state} ->
        {Enum.reverse(frames), next_state}
    end
  end

  test "replays and streams live events from ETS using cursor updates" do
    session_id = unique_id("ses-ets-tail")
    {:ok, _} = Runtime.create_session(id: session_id)

    {:ok, _} =
      Runtime.append_event(session_id, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test"
      })

    {:ok, _} =
      Runtime.append_event(session_id, %{
        type: "content",
        payload: %{text: "two"},
        actor: "agent:test"
      })

    {:ok, initial_state} = TailSocket.init(%{session_id: session_id, cursor: 0})
    {frames, drained_state} = drain_until_idle(initial_state)

    assert frames == [1, 2]
    assert drained_state.cursor == 2

    {:ok, _} =
      Runtime.append_event(session_id, %{
        type: "content",
        payload: %{text: "three"},
        actor: "agent:test"
      })

    assert_receive {:cursor_update, update}, 1_000

    assert {:push, {:text, payload}, next_state} =
             TailSocket.handle_info({:cursor_update, update}, drained_state)

    frame = Jason.decode!(payload)
    assert frame["seq"] == 3
    assert next_state.cursor == 3
  end
end
