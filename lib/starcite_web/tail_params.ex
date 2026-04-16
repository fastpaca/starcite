defmodule StarciteWeb.TailParams do
  @moduledoc """
  Parses canonical Phoenix tail channel parameters.
  """

  alias Starcite.Cursor

  @default_frame_batch_size 1
  @max_frame_batch_size 1_000

  @type parsed :: %{
          required(:cursor) => map(),
          required(:frame_batch_size) => pos_integer(),
          required(:view) => :raw | :composed
        }

  @spec parse(map()) :: {:ok, parsed()} | {:error, atom()}
  def parse(params) when is_map(params) do
    with {:ok, cursor} <- parse_cursor(params),
         {:ok, frame_batch_size} <- parse_frame_batch_size_param(params),
         {:ok, view} <- parse_view_param(params) do
      {:ok, %{cursor: cursor, frame_batch_size: frame_batch_size, view: view}}
    end
  end

  defp parse_cursor(%{"cursor" => cursor}) when is_integer(cursor) and cursor >= 0,
    do: Cursor.normalize(cursor)

  defp parse_cursor(params) when is_map(params) do
    if Map.has_key?(params, "cursor") do
      {:error, :invalid_cursor}
    else
      {:ok, Cursor.new(nil, 0)}
    end
  end

  defp parse_frame_batch_size_param(%{"batch_size" => batch_size}),
    do: parse_frame_batch_size(batch_size)

  defp parse_frame_batch_size_param(%{}), do: {:ok, @default_frame_batch_size}

  defp parse_frame_batch_size(batch_size)
       when is_integer(batch_size) and batch_size >= @default_frame_batch_size and
              batch_size <= @max_frame_batch_size,
       do: {:ok, batch_size}

  defp parse_frame_batch_size(_batch_size), do: {:error, :invalid_tail_batch_size}

  defp parse_view_param(%{"view" => "raw"}), do: {:ok, :raw}
  defp parse_view_param(%{"view" => "composed"}), do: {:ok, :composed}
  defp parse_view_param(%{"view" => _other}), do: {:error, :invalid_tail_view}
  defp parse_view_param(%{}), do: {:ok, :composed}
end
