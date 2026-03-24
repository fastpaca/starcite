defmodule StarciteWeb.TailParams do
  @moduledoc false

  alias Starcite.Cursor

  @default_frame_batch_size 1
  @max_frame_batch_size 1_000

  @type parsed :: %{
          required(:cursor) => map(),
          required(:frame_batch_size) => pos_integer()
        }

  @spec parse(map()) :: {:ok, parsed()} | {:error, atom()}
  def parse(params) when is_map(params) do
    with {:ok, cursor} <- parse_cursor(params),
         {:ok, frame_batch_size} <- parse_frame_batch_size_param(params) do
      {:ok, %{cursor: cursor, frame_batch_size: frame_batch_size}}
    end
  end

  defp parse_cursor(%{"cursor" => nil}), do: {:ok, Cursor.new(nil, 0)}
  defp parse_cursor(%{"cursor" => cursor}), do: Cursor.normalize(cursor)
  defp parse_cursor(%{}), do: {:ok, Cursor.new(nil, 0)}

  defp parse_frame_batch_size_param(%{"batch_size" => batch_size}),
    do: parse_frame_batch_size(batch_size)

  defp parse_frame_batch_size_param(%{}), do: {:ok, @default_frame_batch_size}

  defp parse_frame_batch_size(batch_size)
       when is_integer(batch_size) and batch_size >= @default_frame_batch_size and
              batch_size <= @max_frame_batch_size,
       do: {:ok, batch_size}

  defp parse_frame_batch_size(batch_size) when is_binary(batch_size) do
    case Integer.parse(batch_size) do
      {parsed, ""}
      when parsed >= @default_frame_batch_size and parsed <= @max_frame_batch_size ->
        {:ok, parsed}

      _ ->
        {:error, :invalid_tail_batch_size}
    end
  end

  defp parse_frame_batch_size(_batch_size), do: {:error, :invalid_tail_batch_size}
end
