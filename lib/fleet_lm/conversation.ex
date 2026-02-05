defmodule FleetLM.Conversation do
  @moduledoc """
  Core conversation entity - an append-only, replayable message log.

  This is a message backend substrate. It stores and streams messages but does
  NOT manage prompt windows, token budgets, or compaction. Prompt assembly is
  the responsibility of the client.
  """

  alias __MODULE__, as: Conversation
  alias FleetLM.Conversation.MessageLog

  @type role :: String.t()
  @type part :: %{required(:type) => String.t(), optional(atom()) => term()}
  @type message :: %{
          role: role(),
          parts: [part()],
          seq: non_neg_integer(),
          inserted_at: NaiveDateTime.t(),
          token_count: non_neg_integer(),
          metadata: map()
        }

  @enforce_keys [
    :id,
    :status,
    :message_log,
    :last_seq,
    :archived_seq,
    :version,
    :inserted_at,
    :updated_at
  ]
  defstruct [
    :id,
    :status,
    :message_log,
    :last_seq,
    :archived_seq,
    :version,
    :inserted_at,
    :updated_at,
    :metadata,
    :retention
  ]

  @type t :: %Conversation{
          id: String.t(),
          status: :active | :tombstoned,
          message_log: MessageLog.t(),
          last_seq: non_neg_integer(),
          archived_seq: non_neg_integer(),
          version: non_neg_integer(),
          inserted_at: NaiveDateTime.t(),
          updated_at: NaiveDateTime.t(),
          metadata: map(),
          retention: %{tail_keep: pos_integer()}
        }

  @doc """
  Create a new conversation.

  Options:
    - `status`: :active (default) or :tombstoned
    - `metadata`: optional metadata map
    - `timestamp`: optional creation timestamp
    - `tail_keep`: number of messages to retain after archive ack (default from config)
  """
  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) do
    now = Keyword.get(opts, :timestamp, NaiveDateTime.utc_now())

    tail_keep =
      opts[:tail_keep] || Application.get_env(:fleet_lm, :tail_keep, 1_000)

    %Conversation{
      id: id,
      status: Keyword.get(opts, :status, :active),
      metadata: Keyword.get(opts, :metadata, %{}),
      message_log: MessageLog.new(),
      last_seq: 0,
      archived_seq: 0,
      version: 0,
      inserted_at: now,
      updated_at: now,
      retention: %{tail_keep: tail_keep}
    }
  end

  @doc """
  Update a conversation's status or metadata.
  """
  @spec update(t(), keyword()) :: t()
  def update(%Conversation{} = conv, opts) do
    now = NaiveDateTime.utc_now()

    %Conversation{
      conv
      | status: Keyword.get(opts, :status, conv.status),
        metadata: Keyword.get(opts, :metadata, conv.metadata),
        updated_at: now
    }
  end

  @doc """
  Append messages to the conversation.

  Each input is a map with role, parts, metadata, token_count.
  Returns `{updated_conversation, appended_messages}`.
  """
  @spec append(t(), [map()]) :: {t(), [message()]}
  def append(%Conversation{} = conv, message_inputs) do
    # Convert input maps to tuples for MessageLog
    inbound =
      Enum.map(message_inputs, fn msg ->
        {msg.role, msg.parts, msg.metadata, msg.token_count}
      end)

    {message_log, appended, last_seq} =
      MessageLog.append(conv.message_log, inbound, conv.last_seq)

    new_conv = %Conversation{
      conv
      | message_log: message_log,
        last_seq: last_seq,
        version: conv.version + 1,
        updated_at: NaiveDateTime.utc_now()
    }

    {new_conv, appended}
  end

  @doc """
  Tombstone the conversation (soft delete).

  Tombstoned conversations reject new writes but remain readable.
  """
  @spec tombstone(t()) :: t()
  def tombstone(%Conversation{} = conv),
    do: %Conversation{conv | status: :tombstoned, updated_at: NaiveDateTime.utc_now()}

  @doc """
  Retrieve messages from the tail (newest) with pagination.

  Returns messages in chronological order (oldest to newest within the page).
  """
  @spec messages_tail(t(), non_neg_integer(), pos_integer()) :: [message()]
  def messages_tail(%Conversation{} = conv, offset, limit),
    do: MessageLog.tail_with_offset(conv.message_log, offset, limit)

  @doc """
  Retrieve messages by sequence range for replay.

  Returns messages where `seq >= from_seq`, ordered oldest to newest.
  """
  @spec messages_replay(t(), non_neg_integer(), pos_integer()) :: [message()]
  def messages_replay(%Conversation{} = conv, from_seq, limit),
    do: MessageLog.replay(conv.message_log, from_seq, limit)

  @doc """
  Apply an archive acknowledgement up to `upto_seq`, trimming the log while
  retaining a bounded tail below the boundary.
  """
  @spec persist_ack(t(), non_neg_integer()) :: {t(), non_neg_integer()}
  def persist_ack(%Conversation{} = conv, upto_seq) when is_integer(upto_seq) and upto_seq >= 0 do
    tail_keep = conv.retention.tail_keep
    archived_seq = max(conv.archived_seq, upto_seq)
    {message_log, trimmed} = MessageLog.trim_ack(conv.message_log, archived_seq, tail_keep)

    {%Conversation{
       conv
       | archived_seq: archived_seq,
         message_log: message_log,
         updated_at: NaiveDateTime.utc_now()
     }, trimmed}
  end

  @doc """
  Convert conversation to a map suitable for JSON serialization.
  """
  @spec to_map(t()) :: map()
  def to_map(%Conversation{} = conv) do
    %{
      id: conv.id,
      version: conv.version,
      tombstoned: conv.status == :tombstoned,
      metadata: conv.metadata,
      last_seq: conv.last_seq,
      archived_seq: conv.archived_seq,
      created_at: NaiveDateTime.to_iso8601(conv.inserted_at),
      updated_at: NaiveDateTime.to_iso8601(conv.updated_at)
    }
  end
end
