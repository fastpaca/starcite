defmodule Starcite.Archive.Adapter.S3.Layout do
  @moduledoc """
  Computes S3 keys and chunk boundaries for archive storage.

  Layout is intentionally simple and fixed:
  - Event chunks: `<prefix>/events/v1/<base64url(session_id)>/<chunk_start>.ndjson`
  - Session blobs: `<prefix>/sessions/v1/<base64url(session_id)>.json`
  - One object is one cache-line chunk.

  Chunk boundaries are sequence-based. For a chunk size of `256`, sequence
  `1..256` maps to `1.ndjson`, `257..512` maps to `257.ndjson`, and so on.
  """

  @events_prefix "events/v1"
  @sessions_prefix "sessions/v1"

  @spec group_event_rows([map()], pos_integer()) :: %{{String.t(), pos_integer()} => [map()]}
  def group_event_rows(rows, chunk_size) do
    Enum.group_by(rows, fn %{session_id: session_id, seq: seq} ->
      {session_id, chunk_start_for(seq, chunk_size)}
    end)
  end

  @spec event_chunk_key(map(), String.t(), pos_integer()) :: String.t()
  def event_chunk_key(%{prefix: prefix}, session_id, chunk_start),
    do: "#{prefix}/#{@events_prefix}/#{encode_session_id(session_id)}/#{chunk_start}.ndjson"

  @spec session_prefix(map()) :: String.t()
  def session_prefix(%{prefix: prefix}), do: "#{prefix}/#{@sessions_prefix}/"

  @spec session_key(map(), String.t()) :: String.t()
  def session_key(%{prefix: prefix}, session_id),
    do: "#{prefix}/#{@sessions_prefix}/#{encode_session_id(session_id)}.json"

  @spec chunk_starts_for_range(pos_integer(), pos_integer(), pos_integer()) :: [pos_integer()]
  def chunk_starts_for_range(from_seq, to_seq, chunk_size) do
    first = chunk_start_for(from_seq, chunk_size)
    last = chunk_start_for(to_seq, chunk_size)

    Enum.to_list(first..last//chunk_size)
  end

  @spec chunk_start_for(pos_integer(), pos_integer()) :: pos_integer()
  def chunk_start_for(seq, chunk_size), do: div(seq - 1, chunk_size) * chunk_size + 1

  defp encode_session_id(session_id), do: Base.url_encode64(session_id, padding: false)
end
