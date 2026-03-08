defmodule StarciteWeb.TailParams do
  @moduledoc false

  @default_tail_frame_batch_size 1
  @max_tail_frame_batch_size 1_000

  @spec parse(map()) ::
          {:ok, %{cursor: non_neg_integer(), frame_batch_size: pos_integer()}}
          | {:error, atom()}
  def parse(%{} = params) do
    with {:ok, cursor} <- parse_cursor_param(params),
         {:ok, frame_batch_size} <- parse_frame_batch_size_param(params) do
      {:ok, %{cursor: cursor, frame_batch_size: frame_batch_size}}
    end
  end

  @spec parse_cursor_param(map()) :: {:ok, non_neg_integer()} | {:error, :invalid_cursor}
  def parse_cursor_param(%{"cursor" => cursor}), do: parse_cursor(cursor)
  def parse_cursor_param(%{}), do: {:ok, 0}

  @spec parse_frame_batch_size_param(map()) ::
          {:ok, pos_integer()} | {:error, :invalid_tail_batch_size}
  def parse_frame_batch_size_param(%{"batch_size" => batch_size}),
    do: parse_frame_batch_size(batch_size)

  def parse_frame_batch_size_param(%{}), do: {:ok, @default_tail_frame_batch_size}

  defp parse_cursor(cursor) when is_integer(cursor) and cursor >= 0, do: {:ok, cursor}

  defp parse_cursor(cursor) when is_binary(cursor) do
    case Integer.parse(cursor) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, :invalid_cursor}
    end
  end

  defp parse_cursor(_cursor), do: {:error, :invalid_cursor}

  defp parse_frame_batch_size(batch_size)
       when is_integer(batch_size) and batch_size >= @default_tail_frame_batch_size and
              batch_size <= @max_tail_frame_batch_size,
       do: {:ok, batch_size}

  defp parse_frame_batch_size(batch_size) when is_binary(batch_size) do
    case Integer.parse(batch_size) do
      {parsed, ""}
      when parsed >= @default_tail_frame_batch_size and parsed <= @max_tail_frame_batch_size ->
        {:ok, parsed}

      _ ->
        {:error, :invalid_tail_batch_size}
    end
  end

  defp parse_frame_batch_size(_batch_size), do: {:error, :invalid_tail_batch_size}
end
