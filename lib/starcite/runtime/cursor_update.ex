defmodule Starcite.Runtime.CursorUpdate do
  @moduledoc """
  Payload-free cursor update contract for internal PubSub consumers.

  Topic:
    - `"session_cursor:<session_id>"`

  Message:
    - `{:cursor_update, update_map}`
  """

  @topic_prefix "session_cursor:"

  @type t :: %{
          required(:version) => 1,
          required(:session_id) => String.t(),
          required(:seq) => pos_integer(),
          required(:last_seq) => pos_integer(),
          required(:type) => String.t(),
          required(:actor) => String.t(),
          optional(:source) => String.t() | nil,
          required(:inserted_at) => NaiveDateTime.t() | DateTime.t()
        }

  @type message :: {:cursor_update, t()}

  @spec topic(String.t()) :: String.t()
  def topic(session_id) when is_binary(session_id) and session_id != "" do
    @topic_prefix <> session_id
  end

  @spec message(String.t(), map(), pos_integer()) :: message()
  def message(
        session_id,
        %{
          seq: seq,
          type: type,
          actor: actor,
          inserted_at: inserted_at
        } = event,
        last_seq
      )
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 and
             is_integer(last_seq) and last_seq >= seq and is_binary(type) and type != "" and
             is_binary(actor) and actor != "" and
             (is_struct(inserted_at, NaiveDateTime) or is_struct(inserted_at, DateTime)) do
    source = optional_binary(Map.get(event, :source))

    {:cursor_update,
     %{
       version: 1,
       session_id: session_id,
       seq: seq,
       last_seq: last_seq,
       type: type,
       actor: actor,
       source: source,
       inserted_at: inserted_at
     }}
  end

  defp optional_binary(nil), do: nil
  defp optional_binary(value) when is_binary(value), do: value

  defp optional_binary(value) do
    raise ArgumentError, "expected source to be nil or binary, got: #{inspect(value)}"
  end
end
