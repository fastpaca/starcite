defmodule Starcite.WritePath do
  @moduledoc """
  Write path orchestration for session create, append, and archive-ack operations.
  """

  alias Starcite.WritePath.{Append, ArchiveAck, Create}

  @type ack_archived_entry :: ArchiveAck.ack_archived_entry()
  @type ack_archived_applied :: ArchiveAck.ack_archived_applied()
  @type ack_archived_failed :: ArchiveAck.ack_archived_failed()
  @type ack_archived_result :: ArchiveAck.ack_archived_result()

  @spec create_session(keyword()) :: {:ok, map()} | {:error, term()}
  def create_session(opts \\ []), do: Create.run(opts)

  @doc false
  def create_session_local(id, title, creator_principal, tenant_id, metadata),
    do: Create.run_local(id, title, creator_principal, tenant_id, metadata)

  @spec append_event(String.t(), map()) :: Append.append_result()
  def append_event(id, event), do: Append.append_event(id, event)

  @spec append_event(String.t(), map(), keyword()) :: Append.append_result()
  def append_event(id, event, opts), do: Append.append_event(id, event, opts)

  @doc false
  def append_event_local(id, event), do: Append.append_event_local(id, event)

  @doc false
  def append_event_local(id, event, opts), do: Append.append_event_local(id, event, opts)

  @spec append_events(String.t(), [map()], keyword()) :: Append.append_many_result()
  def append_events(id, events, opts \\ []), do: Append.append_events(id, events, opts)

  @doc false
  def append_events_local(id, events, opts \\ []),
    do: Append.append_events_local(id, events, opts)

  @spec ack_archived([ack_archived_entry()]) ::
          {:ok, ack_archived_result()} | {:error, term()} | {:timeout, term()}
  def ack_archived(entries), do: ArchiveAck.run(entries)

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, ack_archived_result()} | {:error, term()} | {:timeout, term()}
  def ack_archived(id, upto_seq), do: ArchiveAck.run(id, upto_seq)

  @doc false
  def ack_archived_local(entries), do: ArchiveAck.run_local(entries)

  @doc false
  def ack_archived_local(id, upto_seq), do: ArchiveAck.run_local(id, upto_seq)
end
