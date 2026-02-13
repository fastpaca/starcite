defmodule Starcite.Session.ProducerIndex do
  @moduledoc """
  Bounded producer cursor index used for append dedupe.

  State is maintained per session and keyed by `producer_id`.
  Each cursor stores only the latest producer sequence and hash,
  which bounds memory to active producers instead of event volume.
  """

  @type cursor :: %{
          producer_seq: pos_integer(),
          session_seq: pos_integer(),
          hash: binary()
        }

  @type t :: %{optional(String.t()) => cursor()}

  @type decision ::
          {:append, t()}
          | {:deduped, pos_integer(), t()}
          | {:error, :producer_replay_conflict}
          | {:error, {:producer_seq_conflict, String.t(), pos_integer(), pos_integer()}}

  @spec decide(t(), String.t(), pos_integer(), binary(), pos_integer(), pos_integer()) ::
          decision()
  def decide(index, producer_id, producer_seq, hash, next_session_seq, max_entries)
      when is_map(index) and is_binary(producer_id) and producer_id != "" and
             is_integer(producer_seq) and producer_seq > 0 and is_binary(hash) and
             is_integer(next_session_seq) and next_session_seq > 0 and is_integer(max_entries) and
             max_entries > 0 do
    case Map.get(index, producer_id) do
      nil ->
        if producer_seq == 1 do
          {:append,
           put_cursor(index, producer_id, producer_seq, next_session_seq, hash)
           |> prune_lru(max_entries)}
        else
          {:error, {:producer_seq_conflict, producer_id, 1, producer_seq}}
        end

      %{producer_seq: last_producer_seq, session_seq: last_session_seq, hash: last_hash}
      when is_integer(last_producer_seq) and last_producer_seq > 0 and
             is_integer(last_session_seq) and last_session_seq > 0 and is_binary(last_hash) ->
        expected_seq = last_producer_seq + 1

        cond do
          producer_seq == expected_seq ->
            {:append,
             put_cursor(index, producer_id, producer_seq, next_session_seq, hash)
             |> prune_lru(max_entries)}

          producer_seq == last_producer_seq and hash == last_hash ->
            {:deduped, last_session_seq, index}

          producer_seq == last_producer_seq ->
            {:error, :producer_replay_conflict}

          true ->
            {:error, {:producer_seq_conflict, producer_id, expected_seq, producer_seq}}
        end

      _other ->
        {:error, {:producer_seq_conflict, producer_id, 1, producer_seq}}
    end
  end

  @spec prune_lru(t(), pos_integer()) :: t()
  def prune_lru(index, max_entries)
      when is_map(index) and is_integer(max_entries) and max_entries > 0 do
    if map_size(index) <= max_entries do
      index
    else
      index
      |> Enum.sort_by(fn {producer_id, %{session_seq: session_seq}} ->
        {-session_seq, producer_id}
      end)
      |> Enum.take(max_entries)
      |> Map.new()
    end
  end

  defp put_cursor(index, producer_id, producer_seq, session_seq, hash) do
    Map.put(index, producer_id, %{
      producer_seq: producer_seq,
      session_seq: session_seq,
      hash: hash
    })
  end
end
