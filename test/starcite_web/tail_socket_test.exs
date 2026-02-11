defmodule StarciteWeb.TailSocketTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime
  alias Starcite.Runtime.{CursorUpdate, EventStore}
  alias Starcite.Repo
  alias StarciteWeb.TailSocket

  setup do
    Starcite.Runtime.TestHelper.reset()
    :ok
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp base_state(session_id, cursor) do
    %{
      session_id: session_id,
      topic: CursorUpdate.topic(session_id),
      cursor: cursor,
      replay_queue: :queue.new(),
      replay_done: false,
      live_buffer: %{},
      drain_scheduled: false
    }
  end

  defp drain_until_idle(state, frames \\ [], remaining \\ 20)

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
    end
  end

  describe "replay/live boundary" do
    test "replays first, then flushes buffered live events in order without duplicates" do
      session_id = unique_id("ses")
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

      {:ok, state_after_fetch} = TailSocket.handle_info(:drain_replay, base_state(session_id, 0))

      {:ok, _} =
        Runtime.append_event(session_id, %{
          type: "content",
          payload: %{text: "three"},
          actor: "agent:test"
        })

      {:ok, update_three} = cursor_update_for(session_id, 3)

      {:ok, state_with_buffered_live} =
        TailSocket.handle_info({:cursor_update, update_three}, state_after_fetch)

      {frames, drained_state} = drain_until_idle(state_with_buffered_live)

      assert frames == [1, 2, 3]
      assert drained_state.cursor == 3

      assert {:ok, ^drained_state} =
               TailSocket.handle_info({:cursor_update, update_three}, drained_state)
    end

    test "pushes live events immediately after replay is complete" do
      session_id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: session_id)

      {[], drained_state} = drain_until_idle(base_state(session_id, 0))

      {:ok, _} =
        Runtime.append_event(session_id, %{
          type: "state",
          payload: %{state: "running"},
          actor: "agent:test"
        })

      {:ok, update} = cursor_update_for(session_id, 1)

      assert {:push, {:text, payload}, next_state} =
               TailSocket.handle_info({:cursor_update, update}, drained_state)

      frame = Jason.decode!(payload)
      assert frame["seq"] == 1
      assert String.ends_with?(frame["inserted_at"], "Z")
      assert next_state.cursor == 1
    end

    test "replays across Postgres cold + ETS hot boundary" do
      old_archive_enabled = Application.get_env(:starcite, :archive_enabled)
      Application.put_env(:starcite, :archive_enabled, true)

      on_exit(fn ->
        if is_nil(old_archive_enabled) do
          Application.delete_env(:starcite, :archive_enabled)
        else
          Application.put_env(:starcite, :archive_enabled, old_archive_enabled)
        end
      end)

      start_supervised!(Repo)
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
      Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

      session_id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: session_id)

      for n <- 1..4 do
        {:ok, _} =
          Runtime.append_event(session_id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:test"
          })
      end

      cold_rows = EventStore.from_cursor(session_id, 0, 2)
      insert_cold_rows(session_id, cold_rows)
      assert {:ok, %{archived_seq: 2, trimmed: 2}} = Runtime.ack_archived(session_id, 2)

      {frames, final_state} = drain_until_idle(base_state(session_id, 0))
      assert frames == [1, 2, 3, 4]
      assert final_state.cursor == 4
    end
  end

  defp cursor_update_for(session_id, seq) do
    with {:ok, event} <- EventStore.get_event(session_id, seq) do
      {:cursor_update, update} = CursorUpdate.message(session_id, event, seq)
      {:ok, update}
    end
  end

  defp insert_cold_rows(session_id, events) when is_binary(session_id) and is_list(events) do
    rows =
      Enum.map(events, fn event ->
        %{
          session_id: session_id,
          seq: event.seq,
          type: event.type,
          payload: event.payload,
          actor: event.actor,
          source: event.source,
          metadata: event.metadata,
          refs: event.refs,
          idempotency_key: event.idempotency_key,
          inserted_at: as_datetime(event.inserted_at)
        }
      end)

    {count, _} =
      Repo.insert_all(
        "events",
        rows,
        on_conflict: :nothing,
        conflict_target: [:session_id, :seq]
      )

    assert count == length(rows)
  end

  defp as_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
  defp as_datetime(%DateTime{} = value), do: value
end
