defmodule Starcite.Archive.StoreTest do
  use ExUnit.Case, async: false

  alias Starcite.Archive.{IdempotentTestAdapter, Store, TestAdapter}

  setup do
    if pid = Process.whereis(IdempotentTestAdapter) do
      GenServer.stop(pid)
    end

    start_supervised!({IdempotentTestAdapter, []})
    :ok = IdempotentTestAdapter.clear_writes()
    _ = Cachex.clear(:starcite_archive_read_cache)

    previous_adapter = Application.get_env(:starcite, :archive_adapter)
    previous_cache_max_size = Application.get_env(:starcite, :archive_read_cache_max_size)

    previous_cache_reclaim_fraction =
      Application.get_env(:starcite, :archive_read_cache_reclaim_fraction)

    Application.put_env(:starcite, :archive_adapter, IdempotentTestAdapter)

    on_exit(fn ->
      _ = Cachex.clear(:starcite_archive_read_cache)

      if previous_adapter do
        Application.put_env(:starcite, :archive_adapter, previous_adapter)
      else
        Application.delete_env(:starcite, :archive_adapter)
      end

      if is_nil(previous_cache_max_size) do
        Application.delete_env(:starcite, :archive_read_cache_max_size)
      else
        Application.put_env(:starcite, :archive_read_cache_max_size, previous_cache_max_size)
      end

      if is_nil(previous_cache_reclaim_fraction) do
        Application.delete_env(:starcite, :archive_read_cache_reclaim_fraction)
      else
        Application.put_env(
          :starcite,
          :archive_read_cache_reclaim_fraction,
          previous_cache_reclaim_fraction
        )
      end
    end)

    :ok
  end

  test "reads through adapter and caches repeated archive ranges" do
    session_id = "ses-store-#{System.unique_integer([:positive, :monotonic])}"
    rows = build_rows(session_id, 1..3)

    assert {:ok, 3} = Store.write_events(rows)

    assert {:ok, first} = Store.read_events(session_id, 1, 2)
    assert Enum.map(first, & &1.seq) == [1, 2]

    assert {:ok, second} = Store.read_events(session_id, 1, 2)
    assert second == first

    assert IdempotentTestAdapter.get_reads() == [{session_id, 1, 2}]
  end

  test "cache keys are adapter-specific" do
    session_id = "ses-store-adapter-#{System.unique_integer([:positive, :monotonic])}"
    rows = build_rows(session_id, 1..1)

    assert {:ok, 1} = Store.write_events(rows)

    assert {:ok, hot} = Store.read_events(IdempotentTestAdapter, session_id, 1, 1)
    assert Enum.map(hot, & &1.seq) == [1]

    assert {:ok, []} = Store.read_events(TestAdapter, session_id, 1, 1)
  end

  test "cache enforcement evicts when max bytes budget is exceeded" do
    Application.put_env(:starcite, :archive_read_cache_max_size, "1B")
    Application.put_env(:starcite, :archive_read_cache_reclaim_fraction, 0.5)

    session_id = "ses-store-budget-#{System.unique_integer([:positive, :monotonic])}"
    rows = build_rows(session_id, 1..1)
    assert {:ok, 1} = Store.write_events(rows)

    assert {:ok, [%{seq: 1}]} = Store.read_events(session_id, 1, 1)
    assert {:ok, [%{seq: 1}]} = Store.read_events(session_id, 1, 1)

    assert IdempotentTestAdapter.get_reads() == [{session_id, 1, 1}, {session_id, 1, 1}]
  end

  defp build_rows(session_id, seqs) do
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

    Enum.map(seqs, fn seq ->
      %{
        session_id: session_id,
        seq: seq,
        type: "content",
        payload: %{text: "m#{seq}"},
        actor: "agent:test",
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: inserted_at
      }
    end)
  end
end
