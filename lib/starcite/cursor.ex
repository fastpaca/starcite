defmodule Starcite.Cursor do
  @moduledoc """
  Cursor helpers for resume semantics.

  Cursor shape is `%{epoch: integer() | nil, seq: non_neg_integer()}`.
  """

  @type t :: %{
          required(:epoch) => non_neg_integer() | nil,
          required(:seq) => non_neg_integer()
        }

  @spec normalize(term()) :: {:ok, t()} | {:error, :invalid_cursor}
  def normalize(%{seq: seq} = cursor) when is_integer(seq) and seq >= 0 do
    with {:ok, epoch} <- normalize_epoch(Map.get(cursor, :epoch)) do
      {:ok, %{epoch: epoch, seq: seq}}
    end
  end

  def normalize(%{"seq" => seq} = cursor) when is_integer(seq) and seq >= 0 do
    with {:ok, epoch} <- normalize_epoch(Map.get(cursor, "epoch")) do
      {:ok, %{epoch: epoch, seq: seq}}
    end
  end

  def normalize(seq) when is_integer(seq) and seq >= 0 do
    {:ok, %{epoch: nil, seq: seq}}
  end

  def normalize(cursor) when is_binary(cursor) do
    parse_cursor_string(cursor)
  end

  def normalize(_cursor), do: {:error, :invalid_cursor}

  @spec new(non_neg_integer() | nil, non_neg_integer()) :: t()
  def new(epoch, seq) when is_integer(seq) and seq >= 0 do
    normalized_epoch =
      case epoch do
        value when is_integer(value) and value >= 0 -> value
        _ -> nil
      end

    %{epoch: normalized_epoch, seq: seq}
  end

  defp parse_cursor_string(cursor) when is_binary(cursor) do
    trimmed = String.trim(cursor)

    if String.contains?(trimmed, ":") do
      case String.split(trimmed, ":", parts: 2) do
        [epoch_raw, seq_raw] ->
          with {epoch, ""} <- Integer.parse(epoch_raw),
               {seq, ""} <- Integer.parse(seq_raw),
               true <- epoch >= 0 and seq >= 0 do
            {:ok, %{epoch: epoch, seq: seq}}
          else
            _ -> {:error, :invalid_cursor}
          end

        _other ->
          {:error, :invalid_cursor}
      end
    else
      case Integer.parse(trimmed) do
        {seq, ""} when seq >= 0 -> {:ok, %{epoch: nil, seq: seq}}
        _ -> {:error, :invalid_cursor}
      end
    end
  end

  defp normalize_epoch(nil), do: {:ok, nil}
  defp normalize_epoch(epoch) when is_integer(epoch) and epoch >= 0, do: {:ok, epoch}
  defp normalize_epoch(_epoch), do: {:error, :invalid_cursor}
end
