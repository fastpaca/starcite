defmodule Fastpaca.Archive do
  @moduledoc """
  Leader-local archive queue and flush loop.

  - append_messages/2 is invoked by the Raft FSM effects after commits
  - holds pending messages in ETS until flushed to cold storage
  - after flush, advances archive watermark via Runtime.ack_archived/2

  Designed so adapters for Postgres or S3 can be plugged in without touching
  Raft state or HTTP paths.
  """

  use GenServer

  alias Fastpaca.Runtime

  @messages_tab :fastpaca_archive_messages
  @conversations_tab :fastpaca_archive_conversations

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Append committed messages to the pending archive queue (ETS)."
  @spec append_messages(String.t(), [map()]) :: :ok
  def append_messages(conversation_id, messages)
      when is_binary(conversation_id) and is_list(messages) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _pid -> GenServer.cast(__MODULE__, {:append, conversation_id, messages})
    end

    :ok
  end

  @impl true
  def init(opts) do
    # ETS tables (uncapped by design)
    :ets.new(@messages_tab, [:ordered_set, :public, :named_table, {:read_concurrency, true}])
    :ets.new(@conversations_tab, [:set, :public, :named_table, {:read_concurrency, true}])

    interval = Keyword.get(opts, :flush_interval_ms, 5_000)
    adapter_mod = Keyword.fetch!(opts, :adapter)
    adapter_opts = Keyword.get(opts, :adapter_opts, [])

    {:ok, _} = adapter_mod.start_link(adapter_opts)

    state = %{
      interval: interval,
      adapter: adapter_mod,
      adapter_opts: adapter_opts
    }

    schedule_flush(interval)
    {:ok, state}
  end

  @impl true
  def handle_cast({:append, conversation_id, messages}, state) do
    Enum.each(messages, fn msg ->
      key = {conversation_id, msg.seq}
      :ets.insert(@messages_tab, {key, msg})
    end)

    :ets.insert(@conversations_tab, {conversation_id, :pending})
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    flush_all(state)
    schedule_flush(state.interval)
    {:noreply, state}
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_tick, interval)
  end

  defp flush_all(%{adapter: adapter} = state) do
    start = System.monotonic_time(:millisecond)
    conversations = conversations_list()

    {attempted_total, inserted_total, bytes_attempted_total, bytes_inserted_total} =
      Enum.reduce(conversations, {0, 0, 0, 0}, fn conversation_id,
                                                  {attempted_acc, inserted_acc, bytes_att_acc,
                                                   bytes_ins_acc} ->
        rows = take_pending_rows(conversation_id, state)
        attempted = length(rows)
        bytes_attempted = Enum.reduce(rows, 0, fn r, acc -> acc + approx_bytes(r) end)

        if rows != [] do
          case adapter.write_messages(rows) do
            {:ok, _count} ->
              upto_seq = contiguous_upto(rows)

              case Runtime.ack_archived(conversation_id, upto_seq) do
                {:ok, %{archived_seq: _archived, trimmed: _}} ->
                  trim_local(conversation_id, upto_seq)
                  pending_after = pending_count(conversation_id)
                  avg_msg_bytes = if attempted > 0, do: div(bytes_attempted, attempted), else: 0

                  Fastpaca.Observability.Telemetry.archive_batch(
                    conversation_id,
                    attempted,
                    bytes_attempted,
                    avg_msg_bytes,
                    pending_after
                  )

                  maybe_clear_conversation(conversation_id)

                  {attempted_acc + attempted, inserted_acc + attempted,
                   bytes_att_acc + bytes_attempted, bytes_ins_acc + bytes_attempted}

                _ ->
                  {attempted_acc + attempted, inserted_acc, bytes_att_acc + bytes_attempted,
                   bytes_ins_acc}
              end

            {:error, _reason} ->
              {attempted_acc + attempted, inserted_acc, bytes_att_acc + bytes_attempted,
               bytes_ins_acc}
          end
        else
          {attempted_acc, inserted_acc, bytes_att_acc, bytes_ins_acc}
        end
      end)

    elapsed = System.monotonic_time(:millisecond) - start
    pending_rows = :ets.info(@messages_tab, :size) || 0
    pending_conversations = :ets.info(@conversations_tab, :size) || 0

    # Emit queue age gauge
    age_seconds = oldest_age_seconds()
    Fastpaca.Observability.Telemetry.archive_queue_age(age_seconds)

    Fastpaca.Observability.Telemetry.archive_flush(
      elapsed,
      attempted_total,
      inserted_total,
      pending_rows,
      pending_conversations,
      bytes_attempted_total,
      bytes_inserted_total
    )

    :ok
  end

  defp approx_bytes(%{parts: parts, metadata: metadata}) do
    # Approximate payload size by JSON-encoding parts & metadata
    byte_size(Jason.encode!(parts)) + byte_size(Jason.encode!(metadata))
  end

  defp conversations_list do
    :ets.tab2list(@conversations_tab) |> Enum.map(fn {id, _} -> id end)
  end

  defp take_pending_rows(conversation_id, _state) do
    # Select a chunk of rows in seq-ascending order for this conversation
    limit = Application.get_env(:fastpaca, :archive_batch_size, 5_000)

    ms = [
      {
        {{conversation_id, :"$1"}, :"$2"},
        [],
        [{{:"$1", :"$2"}}]
      }
    ]

    select_take(@messages_tab, ms, limit)
    |> Enum.map(fn {seq, msg} ->
      Map.put(msg, :conversation_id, conversation_id) |> Map.put(:seq, seq)
    end)
    |> Enum.sort_by(& &1.seq)
  end

  defp select_take(tab, ms, limit) do
    do_select_take(tab, ms, limit, [])
  end

  defp do_select_take(_tab, _ms, 0, acc), do: Enum.reverse(acc)

  defp do_select_take(tab, ms, remaining, acc) do
    case :ets.select(tab, ms, 1) do
      {[row], cont} ->
        # delete as we go only after ack; for now just accumulate
        # We'll delete after ack succeeds via trim_local/2
        next_acc = [row | acc]
        _ = cont
        # Use continuation to fetch next; but continuation returns tuples of rows
        do_select_take_cont(cont, remaining - 1, next_acc)

      {[], _} ->
        Enum.reverse(acc)

      :"$end_of_table" ->
        Enum.reverse(acc)
    end
  end

  defp do_select_take_cont(:"$end_of_table", _remaining, acc), do: Enum.reverse(acc)
  defp do_select_take_cont(_cont, remaining, acc) when remaining <= 0, do: Enum.reverse(acc)

  defp do_select_take_cont(cont, remaining, acc) when remaining > 0 do
    case :ets.select(cont) do
      {[row], cont2} -> do_select_take_cont(cont2, remaining - 1, [row | acc])
      {[], cont2} -> do_select_take_cont(cont2, 0, acc)
      :"$end_of_table" -> Enum.reverse(acc)
    end
  end

  defp contiguous_upto(rows) do
    rows
    |> Enum.sort_by(& &1.seq)
    |> Enum.reduce({nil, 0}, fn %{seq: seq}, {prev, upto} ->
      cond do
        prev == nil -> {seq, seq}
        seq == prev + 1 -> {seq, seq}
        true -> {prev, upto}
      end
    end)
    |> elem(1)
  end

  defp trim_local(conversation_id, upto_seq) do
    # Delete all rows for this conversation with seq <= upto_seq
    ms = [
      {
        {{conversation_id, :"$1"}, :"$2"},
        [{:"=<", :"$1", upto_seq}],
        [true]
      }
    ]

    :ets.select_delete(@messages_tab, ms)
    :ok
  end

  defp maybe_clear_conversation(conversation_id) do
    # If no rows remain for this conversation, clear it from conversations_tab
    ms = [
      {
        {{conversation_id, :"$1"}, :"$2"},
        [],
        [true]
      }
    ]

    case :ets.select(@messages_tab, ms, 1) do
      {[], _} -> :ets.delete(@conversations_tab, conversation_id)
      _ -> :ok
    end
  end

  defp pending_count(conversation_id) do
    ms = [
      {
        {{conversation_id, :"$1"}, :"$2"},
        [],
        [true]
      }
    ]

    :ets.select_count(@messages_tab, ms)
  end

  defp oldest_age_seconds do
    now = NaiveDateTime.utc_now()
    conversations = conversations_list()

    conversations
    |> Enum.map(&first_row_ts/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(fn ts -> max(NaiveDateTime.diff(now, ts, :second), 0) end)
    |> case do
      [] -> 0
      ages -> Enum.max(ages)
    end
  end

  defp first_row_ts(conversation_id) do
    ms = [
      {
        {{conversation_id, :"$1"}, :"$2"},
        [],
        [:"$2"]
      }
    ]

    case :ets.select(@messages_tab, ms, 1) do
      {[msg], _cont} -> Map.get(msg, :inserted_at)
      _ -> nil
    end
  end
end
