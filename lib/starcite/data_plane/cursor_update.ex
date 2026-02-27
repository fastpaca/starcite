defmodule Starcite.DataPlane.CursorUpdate do
  @moduledoc """
  Cursor update contract for internal PubSub consumers.

  Topic:
    - `"session_cursor:<session_id>"`

  Message:
    - `{:cursor_update, update_map}`
  """

  @topic_prefix "session_cursor:"
  alias Starcite.Session.Event

  @type t :: %{
          required(:version) => 1,
          required(:session_id) => String.t(),
          required(:seq) => pos_integer(),
          required(:last_seq) => pos_integer(),
          required(:type) => String.t(),
          required(:actor) => String.t(),
          optional(:source) => String.t() | nil,
          required(:inserted_at) => NaiveDateTime.t() | DateTime.t(),
          required(:event) => Event.t()
        }

  @type message :: {:cursor_update, t()}

  @spec topic(String.t()) :: String.t()
  def topic(session_id) when is_binary(session_id) and session_id != "" do
    @topic_prefix <> session_id
  end

  @spec message(String.t(), Event.t(), pos_integer()) :: message()
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
      when is_binary(session_id) and session_id != "" and is_integer(last_seq) and last_seq >= 0 do
    {:cursor_update,
     %{
       version: 1,
       session_id: session_id,
       seq: seq,
       last_seq: last_seq,
       type: type,
       actor: actor,
       source: Map.get(event, :source),
       inserted_at: inserted_at,
       event: event
     }}
  end
end
