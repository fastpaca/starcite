defmodule Starcite.Runtime.ArchiveStore do
  @moduledoc """
  Archived event reader backed by Postgres with Cachex fronting.

  This module is used only for cold reads (`seq <= archived_seq`).
  Hot/live reads stay on the ETS event store path.
  """

  import Ecto.Query

  alias Starcite.Archive.Event
  alias Starcite.Repo

  @cache :starcite_archive_read_cache

  @spec from_cursor(String.t(), non_neg_integer(), pos_integer(), non_neg_integer()) ::
          {:ok, [map()]} | {:error, :archive_read_unavailable}
  def from_cursor(session_id, cursor, limit, archived_seq)
      when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
             is_integer(limit) and limit > 0 and is_integer(archived_seq) and archived_seq >= 0 do
    if archived_seq <= cursor do
      {:ok, []}
    else
      lower = cursor + 1
      upper = min(archived_seq, cursor + limit)
      key = {:archive_range, session_id, lower, upper}

      case get_cached(key) do
        {:ok, events} ->
          {:ok, events}

        :miss ->
          with {:ok, events} <- fetch_from_postgres(session_id, lower, upper) do
            :ok = put_cached(key, events)
            {:ok, events}
          end
      end
    end
  end

  @spec available?() :: boolean()
  def available? do
    Process.whereis(Repo) != nil
  end

  defp get_cached(key) do
    case Cachex.get(@cache, key) do
      {:ok, nil} ->
        :miss

      {:ok, events} when is_list(events) ->
        {:ok, events}

      _ ->
        :miss
    end
  end

  defp put_cached(key, events) when is_list(events) do
    _ = Cachex.put(@cache, key, events)
    :ok
  end

  defp fetch_from_postgres(session_id, lower, upper) do
    if available?() do
      query =
        from(e in Event,
          where: e.session_id == ^session_id and e.seq >= ^lower and e.seq <= ^upper,
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
    else
      {:error, :archive_read_unavailable}
    end
  rescue
    _ ->
      {:error, :archive_read_unavailable}
  end
end
