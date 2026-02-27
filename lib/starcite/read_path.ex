defmodule Starcite.ReadPath do
  @moduledoc """
  Read path for session and event tail operations.
  """

  alias Starcite.Archive.Store
  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.{EventStore, RaftAccess, ReplicaRouter, SessionStore}
  alias Starcite.Session

  @default_tail_batch_size 1_000

  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(id) when is_binary(id) and id != "" do
    do_get_session(id)
  end

  def get_session(_id), do: {:error, :invalid_session_id}

  defp do_get_session(id) do
    # Session reads are cache-first so active sessions stay on the low-latency RAM path.
    case SessionStore.get_session(id) do
      {:ok, %Session{} = session} ->
        {:ok, session}

      :error ->
        # On cache miss, hydrate from archive once and warm local cache for follow-up reads.
        with {:ok, %Session{} = session} <- load_session_from_archive(id) do
          :ok = SessionStore.put_session(session)
          {:ok, session}
        end
    end
  end

  defp load_session_from_archive(id) when is_binary(id) and id != "" do
    with {:ok, %{sessions: sessions}} when is_list(sessions) <-
           Store.list_sessions_by_ids([id], %{limit: 1, cursor: nil, metadata: %{}}),
         {:ok, %Session{} = session} <- session_from_archive_rows(id, sessions) do
      {:ok, session}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp session_from_archive_rows(_id, []), do: {:error, :session_not_found}
  defp session_from_archive_rows(id, [row | _rest]), do: session_from_archive_row(id, row)

  defp session_from_archive_row(
         id,
         %{id: id, title: title, metadata: metadata, creator_principal: creator_principal}
       )
       when (is_binary(title) or is_nil(title)) and is_map(metadata) do
    with {:ok, principal} <- archive_creator_principal(creator_principal) do
      {:ok, Session.new(id, title: title, creator_principal: principal, metadata: metadata)}
    end
  end

  defp session_from_archive_row(id, %{id: id, title: title, metadata: metadata})
       when (is_binary(title) or is_nil(title)) and is_map(metadata) do
    {:ok, Session.new(id, title: title, metadata: metadata)}
  end

  defp session_from_archive_row(_id, _row), do: {:error, :archive_read_unavailable}

  defp archive_creator_principal(nil), do: {:ok, nil}
  defp archive_creator_principal(%Principal{} = principal), do: {:ok, principal}

  defp archive_creator_principal(%{
         "tenant_id" => tenant_id,
         "id" => principal_id,
         "type" => type
       })
       when is_binary(tenant_id) and tenant_id != "" and is_binary(principal_id) and
              principal_id != "" do
    with {:ok, principal_type} <- principal_type(type),
         {:ok, principal} <- Principal.new(tenant_id, principal_id, principal_type) do
      {:ok, principal}
    end
  end

  defp archive_creator_principal(%{tenant_id: tenant_id, id: principal_id, type: type})
       when is_binary(tenant_id) and tenant_id != "" and is_binary(principal_id) and
              principal_id != "" do
    with {:ok, principal_type} <- principal_type(type),
         {:ok, principal} <- Principal.new(tenant_id, principal_id, principal_type) do
      {:ok, principal}
    else
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp archive_creator_principal(_other), do: {:error, :archive_read_unavailable}

  defp principal_type("user"), do: {:ok, :user}
  defp principal_type("agent"), do: {:ok, :agent}
  defp principal_type(:user), do: {:ok, :user}
  defp principal_type(:agent), do: {:ok, :agent}
  defp principal_type(_other), do: :error

  @spec get_events_from_cursor(String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def get_events_from_cursor(id, cursor, limit \\ @default_tail_batch_size)

  def get_events_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    group = RaftAccess.group_for_session(id)

    ReplicaRouter.call_on_replica(
      group,
      __MODULE__,
      :rpc_get_events_from_cursor,
      [id, cursor, limit],
      __MODULE__,
      :rpc_get_events_from_cursor,
      [id, cursor, limit],
      prefer_leader: false
    )
  end

  def get_events_from_cursor(_id, _cursor, _limit), do: {:error, :invalid_cursor}

  @doc false
  def rpc_get_events_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    do_get_events_from_cursor(id, cursor, limit)
  end

  defp do_get_events_from_cursor(id, cursor, limit) do
    with {:ok, hot_events} <- read_hot_events(id, cursor, limit),
         {:ok, cold_events} <- maybe_read_cold_events(id, cursor, limit, hot_events),
         {:ok, merged} <- merge_events(cold_events, hot_events, limit) do
      {:ok, merged}
    else
      {:error, :archive_read_unavailable} ->
        with {:ok, hot_events} <- read_hot_events(id, cursor, limit),
             {:ok, merged} <- merge_events([], hot_events, limit) do
          {:ok, merged}
        end
    end
  end

  defp maybe_read_cold_events(id, cursor, limit, []),
    do: read_cold_events(id, cursor, limit)

  defp maybe_read_cold_events(_id, cursor, _limit, [%{seq: seq} | _rest])
       when is_integer(cursor) and is_integer(seq) and seq == cursor + 1 do
    {:ok, []}
  end

  defp maybe_read_cold_events(id, cursor, limit, _hot_events) do
    read_cold_events(id, cursor, limit)
  end

  defp read_cold_events(id, cursor, limit) do
    from_seq = cursor + 1
    to_seq = cursor + limit

    try do
      EventStore.read_archived_events(id, from_seq, to_seq)
    rescue
      DBConnection.OwnershipError -> {:error, :archive_read_unavailable}
    end
  end

  defp read_hot_events(id, cursor, limit) do
    {:ok, EventStore.from_cursor(id, cursor, limit)}
  end

  defp merge_events(cold_events, hot_events, limit) do
    merged =
      (cold_events ++ hot_events)
      |> Enum.uniq_by(& &1.seq)
      |> Enum.sort_by(& &1.seq)
      |> Enum.take(limit)

    ensure_gap_free(merged)
  end

  defp ensure_gap_free([]), do: {:ok, []}

  defp ensure_gap_free([_single] = events), do: {:ok, events}

  defp ensure_gap_free(events) do
    gap? =
      events
      |> Enum.reduce_while(nil, fn event, previous_seq ->
        cond do
          previous_seq == nil ->
            {:cont, event.seq}

          event.seq == previous_seq + 1 ->
            {:cont, event.seq}

          true ->
            {:halt, :gap}
        end
      end)
      |> Kernel.==(:gap)

    if gap? do
      {:error, :event_gap_detected}
    else
      {:ok, events}
    end
  end
end
