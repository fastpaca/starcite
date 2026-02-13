defmodule Starcite.Archive.Adapter.Postgres do
  @moduledoc """
  Postgres archive adapter using Ecto.

  Inserts events in batches with ON CONFLICT DO NOTHING for idempotency.
  """

  @behaviour Starcite.Archive.Adapter

  use GenServer

  import Ecto.Query

  alias Starcite.Archive.Event
  alias Starcite.Repo

  @impl true
  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(state), do: {:ok, state}

  # Postgres parameter limit is 65535. With 10 fields per event,
  # we can safely insert ~6000 rows per batch. Use 5000 to be safe.
  @chunk_size 5_000

  @impl true
  def write_events(rows) when is_list(rows) do
    if rows == [] do
      {:ok, 0}
    else
      total_count =
        rows
        |> Enum.chunk_every(@chunk_size)
        |> Enum.reduce(0, fn chunk, acc ->
          {count, _} = insert_all_with_conflict(chunk)
          acc + count
        end)

      {:ok, total_count}
    end
  end

  @impl true
  def read_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    query =
      from(e in Event,
        where: e.session_id == ^session_id and e.seq >= ^from_seq and e.seq <= ^to_seq,
        order_by: [asc: e.seq],
        select: %{
          seq: e.seq,
          type: e.type,
          payload: e.payload,
          actor: e.actor,
          source: e.source,
          metadata: e.metadata,
          refs: e.refs,
          idempotency_key: e.idempotency_key,
          inserted_at: e.inserted_at
        }
      )

    {:ok, Repo.all(query)}
  end

  defp insert_all_with_conflict(rows) do
    entries =
      Enum.map(rows, fn row ->
        %{
          session_id: row.session_id,
          seq: row.seq,
          type: row.type,
          actor: row.actor,
          source: row.source,
          payload: row.payload,
          metadata: row.metadata,
          refs: row.refs,
          idempotency_key: row.idempotency_key,
          inserted_at: normalize_inserted_at(row.inserted_at)
        }
      end)

    Repo.insert_all(
      Event,
      entries,
      on_conflict: :nothing,
      conflict_target: [:session_id, :seq]
    )
  end

  defp normalize_inserted_at(%NaiveDateTime{} = naive_datetime) do
    naive_datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.truncate(:second)
  end

  defp normalize_inserted_at(%DateTime{} = datetime), do: DateTime.truncate(datetime, :second)
end
