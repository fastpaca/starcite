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
    group = RaftAccess.group_for_session(id)

    ReplicaRouter.call_on_replica(
      group,
      __MODULE__,
      :rpc_get_session,
      [id],
      __MODULE__,
      :rpc_get_session,
      [id],
      prefer_leader: false
    )
  end

  def get_session(_id), do: {:error, :invalid_session_id}

  @doc false
  def rpc_get_session(id) when is_binary(id) and id != "" do
    do_get_session(id)
  end

  defp do_get_session(id) do
    case SessionStore.get_session(id) do
      {:ok, %Session{} = session} ->
        {:ok, session}

      :error ->
        with {:ok, %Session{} = session} <- load_session_from_archive(id) do
          :ok = SessionStore.put_session(session)
          {:ok, session}
        end
    end
  end

  defp load_session_from_archive(id) when is_binary(id) and id != "" do
    with {:ok, page} <- Store.list_sessions_by_ids([id], %{limit: 1, cursor: nil, metadata: %{}}),
         {:ok, row} <- archive_session_row(page, id),
         {:ok, creator_principal} <- archive_creator_principal(row),
         {:ok, metadata} <- archive_metadata(row) do
      title = archive_title(row)

      {:ok,
       Session.new(id, title: title, creator_principal: creator_principal, metadata: metadata)}
    end
  end

  defp archive_session_row(%{sessions: [%{id: id} = row | _rest]}, id), do: {:ok, row}

  defp archive_session_row(%{sessions: [_other | _rest]}, _id),
    do: {:error, :archive_read_unavailable}

  defp archive_session_row(%{sessions: []}, _id), do: {:error, :session_not_found}

  defp archive_title(%{title: title}) when is_binary(title) or is_nil(title), do: title
  defp archive_title(_row), do: nil

  defp archive_metadata(%{metadata: metadata}) when is_map(metadata), do: {:ok, metadata}
  defp archive_metadata(_row), do: {:ok, %{}}

  defp archive_creator_principal(%{creator_principal: nil}), do: {:ok, nil}

  defp archive_creator_principal(%{creator_principal: %Principal{} = principal}),
    do: {:ok, principal}

  defp archive_creator_principal(%{creator_principal: creator}) when is_map(creator) do
    with {:ok, tenant_id} <- fetch_creator_field(creator, "tenant_id"),
         {:ok, principal_id} <- fetch_creator_field(creator, "id"),
         {:ok, type} <- fetch_creator_field(creator, "type"),
         {:ok, principal_type} <- creator_type(type),
         {:ok, principal} <- Principal.new(tenant_id, principal_id, principal_type) do
      {:ok, principal}
    else
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp archive_creator_principal(_row), do: {:ok, nil}

  defp fetch_creator_field(map, field) when is_map(map) and is_binary(field) do
    case field do
      "tenant_id" -> fetch_creator_field_value(map, "tenant_id", :tenant_id)
      "id" -> fetch_creator_field_value(map, "id", :id)
      "type" -> fetch_creator_field_value(map, "type", :type)
      _other -> :error
    end
  end

  defp fetch_creator_field_value(map, string_key, atom_key) when is_map(map) do
    value = Map.get(map, string_key) || Map.get(map, atom_key)
    if is_binary(value) and value != "", do: {:ok, value}, else: :error
  end

  defp creator_type("user"), do: {:ok, :user}
  defp creator_type("agent"), do: {:ok, :agent}
  defp creator_type(_other), do: :error

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
