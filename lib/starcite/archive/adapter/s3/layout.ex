defmodule Starcite.Archive.Adapter.S3.Layout do
  @moduledoc false

  @events_prefix "events/v1"
  @sessions_prefix "sessions/v1"

  @spec group_event_rows([map()], pos_integer()) :: %{{String.t(), pos_integer()} => [map()]}
  def group_event_rows(rows, chunk_size) do
    Enum.group_by(rows, fn %{session_id: session_id, seq: seq} ->
      {session_id, chunk_start_for(seq, chunk_size)}
    end)
  end

  @spec event_chunk_key(map(), String.t(), pos_integer()) :: String.t()
  def event_chunk_key(config, session_id, chunk_start) do
    join([
      config.prefix,
      @events_prefix,
      encode_session_id(session_id),
      "#{chunk_start}.ndjson"
    ])
  end

  @spec session_prefix(map()) :: String.t()
  def session_prefix(config), do: join([config.prefix, @sessions_prefix]) <> "/"

  @spec session_key(map(), String.t()) :: String.t()
  def session_key(config, session_id) do
    join([config.prefix, @sessions_prefix, "#{encode_session_id(session_id)}.json"])
  end

  @spec chunk_starts_for_range(pos_integer(), pos_integer(), pos_integer()) :: [pos_integer()]
  def chunk_starts_for_range(from_seq, to_seq, chunk_size) do
    first = chunk_start_for(from_seq, chunk_size)
    last = chunk_start_for(to_seq, chunk_size)

    first
    |> Stream.iterate(&(&1 + chunk_size))
    |> Enum.take_while(&(&1 <= last))
  end

  @spec chunk_start_for(pos_integer(), pos_integer()) :: pos_integer()
  def chunk_start_for(seq, chunk_size), do: div(seq - 1, chunk_size) * chunk_size + 1

  defp join(parts) do
    parts
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&to_string/1)
    |> Enum.map(&String.trim(&1, "/"))
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("/")
  end

  defp encode_session_id(session_id), do: Base.url_encode64(session_id, padding: false)
end
