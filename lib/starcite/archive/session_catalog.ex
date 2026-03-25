defmodule Starcite.Archive.SessionCatalog do
  @moduledoc """
  Persisted session catalog boundary.

  Owns the archived session row contract used for create persistence and
  hydrate-on-miss recovery.
  """

  alias Starcite.Archive.Store
  alias Starcite.Auth.Principal
  alias Starcite.Session

  @spec persist_created(map(), Principal.t(), String.t(), map()) :: :ok | {:error, term()}
  def persist_created(
        %{id: id, title: title, created_at: created_at},
        %Principal{} = creator_principal,
        tenant_id,
        metadata
      )
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    Store.upsert_session(%{
      id: id,
      title: title,
      creator_principal: creator_principal,
      tenant_id: tenant_id,
      metadata: metadata,
      archived_seq: 0,
      created_at: created_at
    })
  end

  def persist_created(_session, _creator_principal, _tenant_id, _metadata),
    do: {:error, :invalid_session}

  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, %{sessions: sessions}} when is_list(sessions) <-
           Store.list_sessions_by_ids([session_id], %{limit: 1, cursor: nil, metadata: %{}}),
         {:ok, row} <- select_row(session_id, sessions),
         {:ok, archived_seq} <- archived_seq_for_row(session_id, row),
         {:ok, session} <- row_to_session(session_id, row, archived_seq) do
      {:ok, session}
    end
  end

  def get_session(_session_id), do: {:error, :invalid_session_id}

  defp select_row(_session_id, []), do: {:error, :session_not_found}

  defp select_row(session_id, [%{id: session_id} = row | _rest])
       when is_binary(session_id) and session_id != "" do
    {:ok, row}
  end

  defp select_row(_session_id, _rows), do: {:error, :archive_read_unavailable}

  defp row_to_session(
         session_id,
         %{
           id: session_id,
           title: title,
           tenant_id: tenant_id,
           creator_principal: creator_principal,
           metadata: metadata,
           created_at: created_at
         },
         archived_seq
       )
       when is_binary(tenant_id) and tenant_id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) and is_integer(archived_seq) and archived_seq >= 0 do
    with {:ok, timestamp} <- session_created_at(created_at) do
      try do
        session =
          Session.new(session_id,
            title: title,
            tenant_id: tenant_id,
            creator_principal: creator_principal,
            metadata: metadata,
            timestamp: timestamp
          )

        {:ok, %Session{session | last_seq: archived_seq, archived_seq: archived_seq}}
      rescue
        ArgumentError -> {:error, :archive_read_unavailable}
      end
    end
  end

  defp row_to_session(_session_id, _row, _archived_seq), do: {:error, :archive_read_unavailable}

  defp archived_seq_for_row(
         session_id,
         %{tenant_id: tenant_id} = row
       )
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    with {:ok, derived_archived_seq} <- Store.archived_seq(session_id, tenant_id) do
      row_archived_seq =
        case Map.get(row, :archived_seq) do
          value when is_integer(value) and value >= 0 -> value
          _other -> 0
        end

      {:ok, max(derived_archived_seq, row_archived_seq)}
    end
  end

  defp archived_seq_for_row(_session_id, _row), do: {:error, :archive_read_unavailable}

  defp session_created_at(%DateTime{} = created_at), do: {:ok, DateTime.to_naive(created_at)}
  defp session_created_at(%NaiveDateTime{} = created_at), do: {:ok, created_at}

  defp session_created_at(created_at) when is_binary(created_at) do
    case DateTime.from_iso8601(created_at) do
      {:ok, datetime, _offset} ->
        {:ok, DateTime.to_naive(datetime)}

      _ ->
        case NaiveDateTime.from_iso8601(created_at) do
          {:ok, datetime} -> {:ok, datetime}
          _ -> {:error, :archive_read_unavailable}
        end
    end
  end

  defp session_created_at(_created_at), do: {:error, :archive_read_unavailable}
end
