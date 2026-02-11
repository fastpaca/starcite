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
    if Process.whereis(Repo) == nil do
      {:ok, []}
    else
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
  rescue
    _ -> {:error, :archive_read_unavailable}
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
          payload: Jason.encode!(row.payload),
          metadata: Jason.encode!(row.metadata),
          refs: Jason.encode!(row.refs),
          idempotency_key: row.idempotency_key,
          inserted_at: DateTime.from_naive!(row.inserted_at, "Etc/UTC")
        }
      end)

    Repo.insert_all(
      "events",
      entries,
      placeholders: %{payload: :string, metadata: :string, refs: :string},
      on_conflict: :nothing,
      conflict_target: [:session_id, :seq]
    )
  end
end
