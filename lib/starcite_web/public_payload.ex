defmodule StarciteWeb.PublicPayload do
  @moduledoc false

  @spec append_reply(map()) :: map()
  def append_reply(%{results: results} = reply) when is_list(results) do
    reply
    |> strip_epoch()
    |> put_cursor(:cursor, Map.get(reply, :cursor))
    |> put_cursor(:committed_cursor, Map.get(reply, :committed_cursor))
    |> Map.put(:results, Enum.map(results, &append_reply/1))
  end

  def append_reply(reply) when is_map(reply) do
    reply
    |> strip_epoch()
    |> put_cursor(:cursor, Map.get(reply, :cursor))
    |> put_cursor(:committed_cursor, Map.get(reply, :committed_cursor))
  end

  @spec event(map()) :: map()
  def event(%{seq: seq} = event) when is_integer(seq) and seq >= 0 do
    event
    |> strip_epoch()
    |> Map.put(:cursor, seq)
  end

  def event(event) when is_map(event), do: strip_epoch(event)

  @spec gap(map()) :: map()
  def gap(gap) when is_map(gap) do
    %{
      type: "gap",
      reason: gap_reason(Map.get(gap, :reason)),
      from_cursor: cursor(Map.get(gap, :from_cursor)),
      next_cursor: cursor(Map.get(gap, :next_cursor)),
      committed_cursor: cursor(Map.get(gap, :committed_cursor)),
      earliest_available_cursor: cursor(Map.get(gap, :earliest_available_cursor))
    }
  end

  @spec gap_reason(term()) :: String.t()
  def gap_reason(reason) when reason in [:epoch_stale, :rollback], do: "resume_invalidated"
  def gap_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  def gap_reason(reason) when is_binary(reason), do: reason
  def gap_reason(_reason), do: "resume_invalidated"

  @spec cursor(term()) :: non_neg_integer() | nil
  def cursor(%{seq: seq}) when is_integer(seq) and seq >= 0, do: seq
  def cursor(%{"seq" => seq}) when is_integer(seq) and seq >= 0, do: seq
  def cursor(seq) when is_integer(seq) and seq >= 0, do: seq
  def cursor(nil), do: nil
  def cursor(_value), do: nil

  defp put_cursor(payload, key, value) when is_map(payload) and is_atom(key) do
    case cursor(value) do
      nil -> Map.delete(payload, key)
      seq -> Map.put(payload, key, seq)
    end
  end

  defp strip_epoch(payload) when is_map(payload), do: Map.delete(payload, :epoch)
end
