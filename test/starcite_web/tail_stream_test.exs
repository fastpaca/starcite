defmodule StarciteWeb.TailStreamTest do
  use ExUnit.Case, async: false

  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.{CursorUpdate, EventStore}
  alias Starcite.Storage.EventArchive
  alias Starcite.WritePath
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.TailStream

  setup do
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})
    :ok
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  defp base_state(session_id, cursor, principal \\ nil) do
    {:ok, state} =
      TailStream.init(%{
        session_id: session_id,
        cursor: %{epoch: nil, seq: cursor},
        frame_batch_size: 1,
        principal: principal,
        auth_context: %Context{
          kind: :jwt,
          principal: principal || %Principal{tenant_id: "acme", id: "tail-test", type: :service},
          scopes: ["session:read"],
          session_id: nil,
          expires_at: nil
        }
      })

    state
  end

  defp drain_until_idle(state, frames \\ [], remaining \\ 20)

  defp drain_until_idle(state, frames, 0), do: {Enum.reverse(frames), state}

  defp drain_until_idle(state, frames, remaining) do
    case TailStream.handle_info(:drain_replay, state) do
      {:emit, {:events, events}, next_state} ->
        seqs = Enum.map(events, & &1.seq)
        drain_until_idle(next_state, Enum.reverse(seqs) ++ frames, remaining - 1)

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

  test "replays first, then flushes buffered live events in order without duplicates" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    {:ok, state_after_fetch} = TailStream.handle_info(:drain_replay, base_state(session_id, 0))

    {:ok, _} =
      append_event(session_id, %{
        type: "content",
        payload: %{text: "three"},
        actor: "agent:test"
      })

    {:ok, update_three} = cursor_update_for(session_id, 3)

    {:ok, state_with_buffered_live} =
      TailStream.handle_info({:cursor_update, update_three}, state_after_fetch)

    {frames, drained_state} = drain_until_idle(state_with_buffered_live)

    assert frames == [1, 2, 3]
    assert drained_state.cursor == 3

    assert {:ok, ^drained_state} =
             TailStream.handle_info({:cursor_update, update_three}, drained_state)
  end

  test "emits live events immediately after replay is complete" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id)

    {[], drained_state} = drain_until_idle(base_state(session_id, 0))

    {:ok, _} =
      append_event(session_id, %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:test"
      })

    {:ok, update} = cursor_update_for(session_id, 1)

    assert {:emit, {:events, [%{seq: 1} = event]}, next_state} =
             TailStream.handle_info({:cursor_update, update}, drained_state)

    assert event.payload == %{state: "running"}
    assert event.cursor == 1
    refute Map.has_key?(event, :epoch)
    assert String.ends_with?(event.inserted_at, "Z")
    assert next_state.cursor == 1
  end

  test "falls back to storage when a live cursor update misses ETS" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    {:ok, _} =
      append_event(session_id, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test"
      })

    cold_rows = EventStore.from_cursor(session_id, 0, 1)
    insert_cold_rows(session_id, cold_rows)
    assert {:ok, %{archived_seq: 1, trimmed: 1}} = WritePath.ack_archived(session_id, 1)
    assert :error = EventStore.get_event(session_id, 1)

    update = %{
      version: 1,
      session_id: session_id,
      tenant_id: "acme",
      seq: 1,
      last_seq: 1,
      published_at_ms: System.system_time(:millisecond),
      type: "content",
      actor: "agent:test",
      source: nil,
      inserted_at: NaiveDateTime.utc_now()
    }

    state = %{base_state(session_id, 0, principal_for_tenant("acme")) | replay_done: true}

    assert {:emit, {:events, [%{seq: 1} = event]}, next_state} =
             TailStream.handle_info({:cursor_update, update}, state)

    assert event.payload == %{"text" => "one"}
    assert next_state.cursor == 1
  end

  test "maps stale epoch replay gaps to a public resume_invalidated reason" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id)

    {:ok, _} =
      append_event(session_id, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test"
      })

    stale_epoch_state = %{base_state(session_id, 10) | cursor_epoch: 99}

    assert {:emit, {:gap, gap}, next_state} =
             TailStream.handle_info(:drain_replay, stale_epoch_state)

    assert gap.reason == "resume_invalidated"
    assert gap.from_cursor == 10
    assert gap.next_cursor == 1
    assert next_state.cursor == 1
    assert is_integer(next_state.cursor_epoch)
    assert next_state.cursor_epoch >= 0
  end

  test "emits resume_invalidated when a public seq cursor is ahead of the active head" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id)

    {:ok, _} =
      append_event(session_id, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test"
      })

    assert {:emit, {:gap, gap}, next_state} =
             TailStream.handle_info(:drain_replay, base_state(session_id, 10))

    assert gap.reason == "resume_invalidated"
    assert gap.from_cursor == 10
    assert gap.next_cursor == 1
    assert next_state.cursor == 1
  end

  test "emits cursor_expired gap when cursor falls below available floor" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id)

    for n <- 1..3 do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "m#{n}"},
          actor: "agent:test"
        })
    end

    assert {:ok, %{archived_seq: 2, trimmed: 2}} = WritePath.ack_archived(session_id, 2)

    assert {:emit, {:gap, gap}, next_state} =
             TailStream.handle_info(:drain_replay, base_state(session_id, 0))

    assert gap.reason == "cursor_expired"
    assert gap.from_cursor == 0
    assert gap.next_cursor == 2
    assert next_state.cursor == 2
  end

  test "emits token_expired when auth lifetime expires" do
    state = %{base_state("ses-auth", 0) | replay_done: true}
    assert {:token_expired, ^state} = TailStream.handle_info(:auth_expired, state)
  end

  defp append_event(id, event, opts \\ [])
       when is_binary(id) and is_map(event) and is_list(opts) do
    producer_id = Map.get(event, :producer_id, "writer:test")

    enriched_event =
      event
      |> Map.put_new(:producer_id, producer_id)
      |> Map.put_new_lazy(:producer_seq, fn -> next_producer_seq(id, producer_id) end)

    WritePath.append_event(id, enriched_event, opts)
  end

  defp next_producer_seq(session_id, producer_id)
       when is_binary(session_id) and is_binary(producer_id) do
    counters = Process.get(:producer_seq_counters, %{})
    key = {session_id, producer_id}
    seq = Map.get(counters, key, 0) + 1
    Process.put(:producer_seq_counters, Map.put(counters, key, seq))
    seq
  end

  defp principal_for_tenant(tenant_id) when is_binary(tenant_id) and tenant_id != "" do
    %Principal{tenant_id: tenant_id, id: "tail-test", type: :service}
  end

  defp cursor_update_for(session_id, seq) do
    with {:ok, event} <- EventStore.get_event(session_id, seq) do
      {:cursor_update, update} =
        CursorUpdate.message(session_id, event_tenant_id!(event), event, seq)

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
          producer_id: event.producer_id,
          producer_seq: event.producer_seq,
          tenant_id: event_tenant_id!(event),
          metadata: Map.get(event, :metadata, %{}),
          refs: Map.get(event, :refs, %{}),
          source: Map.get(event, :source),
          idempotency_key: Map.get(event, :idempotency_key),
          inserted_at: as_datetime(event.inserted_at)
        }
      end)

    assert {:ok, inserted} = EventArchive.write_events(rows)
    assert inserted == length(rows)
  end

  defp event_tenant_id!(event) when is_map(event) do
    case Map.fetch(event, :tenant_id) do
      {:ok, tenant_id} when is_binary(tenant_id) -> tenant_id
      _ -> "acme"
    end
  end

  defp as_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
  defp as_datetime(%DateTime{} = value), do: value
end
