defmodule Starcite.Storage.SessionCatalog do
  @moduledoc """
  Durable session catalog backed by `Starcite.Repo`.

  This boundary owns persisted session headers, session listing, and archived
  progress. Event archive adapters are not consulted for session metadata.
  """

  import Ecto.Query

  alias Starcite.Repo
  alias Starcite.Auth.Principal
  alias Starcite.Session
  alias Starcite.Session.Header
  alias Starcite.Storage.SessionRecord

  @progress_batch_size 5_000

  @type session_query :: %{
          optional(:limit) => pos_integer(),
          optional(:cursor) => String.t() | nil,
          optional(:metadata) => map(),
          optional(:owner_principal_ids) => [String.t()],
          optional(:tenant_id) => String.t()
        }

  @type session_page :: %{
          required(:sessions) => [map()],
          required(:next_cursor) => String.t() | nil
        }

  @type progress_update :: %{
          required(:session_id) => String.t(),
          required(:archived_seq) => non_neg_integer()
        }

  @spec persist_created(Header.t()) :: :ok | {:error, term()}
  def persist_created(%Header{
        id: id,
        title: title,
        creator_principal: creator_principal,
        tenant_id: tenant_id,
        metadata: metadata,
        created_at: created_at
      })
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    {creator_id, creator_type} = creator_identity(creator_principal)

    {_inserted, _} =
      Repo.insert_all(
        "sessions",
        [
          %{
            id: id,
            title: title,
            tenant_id: tenant_id,
            creator_id: creator_id,
            creator_type: creator_type,
            metadata: metadata,
            archived_seq: 0,
            created_at:
              created_at |> DateTime.from_naive!("Etc/UTC") |> DateTime.truncate(:second)
          }
        ],
        on_conflict: :nothing,
        conflict_target: [:id]
      )

    :ok
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

  def persist_created(_header), do: {:error, :invalid_session}

  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, row} <- get_record(session_id),
         {:ok, header} <- row_to_header(session_id, row),
         archived_seq when is_integer(archived_seq) and archived_seq >= 0 <- row.archived_seq do
      {:ok, Session.hydrate(header, archived_seq)}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :session_catalog_unavailable}
    end
  end

  def get_session(_session_id), do: {:error, :invalid_session_id}

  @spec get_header(String.t()) :: {:ok, Header.t()} | {:error, term()}
  def get_header(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, row} <- get_record(session_id) do
      row_to_header(session_id, row)
    end
  end

  def get_header(_session_id), do: {:error, :invalid_session_id}

  @spec get_tenant_id(String.t()) :: {:ok, String.t()} | {:error, term()}
  def get_tenant_id(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, row} <- get_record(session_id),
         tenant_id when is_binary(tenant_id) and tenant_id != "" <- row.tenant_id do
      {:ok, tenant_id}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :session_catalog_unavailable}
    end
  end

  def get_tenant_id(_session_id), do: {:error, :invalid_session_id}

  @spec get_progress(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def get_progress(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, row} <- get_record(session_id),
         archived_seq when is_integer(archived_seq) and archived_seq >= 0 <- row.archived_seq do
      {:ok, archived_seq}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :session_catalog_unavailable}
    end
  end

  def get_progress(_session_id), do: {:error, :invalid_session_id}

  @spec put_progress_batch([progress_update()]) :: :ok | {:error, term()}
  def put_progress_batch(progress_updates) when is_list(progress_updates) do
    progress_updates
    |> normalize_progress_updates()
    |> Enum.chunk_every(@progress_batch_size)
    |> Enum.reduce_while(:ok, fn
      [], :ok ->
        {:cont, :ok}

      chunk, :ok ->
        case put_progress_chunk(chunk) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
  end

  @spec list_sessions(session_query()) :: {:ok, session_page()} | {:error, term()}
  def list_sessions(%{limit: limit, cursor: cursor, metadata: metadata} = query_opts)
      when is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    query =
      SessionRecord
      |> apply_cursor(cursor)
      |> apply_tenant_filter(Map.get(query_opts, :tenant_id))
      |> apply_owner_principal_filters(Map.get(query_opts, :owner_principal_ids))
      |> apply_metadata_filters(metadata)
      |> order_by([s], asc: s.id)
      |> limit(^limit)

    rows = Repo.all(query)
    build_session_page(rows, limit)
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

  def list_sessions(_query_opts), do: {:error, :invalid_list_query}

  @spec list_sessions_by_ids([String.t()], session_query()) ::
          {:ok, session_page()} | {:error, term()}
  def list_sessions_by_ids(ids, %{limit: limit, cursor: cursor, metadata: metadata} = query_opts)
      when is_list(ids) and is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    session_ids = ids |> Enum.uniq() |> Enum.reject(&(&1 == ""))

    if session_ids == [] do
      {:ok, %{sessions: [], next_cursor: nil}}
    else
      query =
        SessionRecord
        |> where([s], s.id in ^session_ids)
        |> apply_cursor(cursor)
        |> apply_tenant_filter(Map.get(query_opts, :tenant_id))
        |> apply_owner_principal_filters(Map.get(query_opts, :owner_principal_ids))
        |> apply_metadata_filters(metadata)
        |> order_by([s], asc: s.id)
        |> limit(^limit)

      rows = Repo.all(query)
      build_session_page(rows, limit)
    end
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

  def list_sessions_by_ids(_ids, _query_opts), do: {:error, :invalid_list_query}

  defp get_record(session_id) when is_binary(session_id) and session_id != "" do
    case Repo.get(SessionRecord, session_id) do
      %SessionRecord{} = row -> {:ok, row}
      nil -> {:error, :session_not_found}
    end
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

  defp normalize_progress_updates(progress_updates) when is_list(progress_updates) do
    progress_updates
    |> Enum.reduce(%{}, fn
      %{session_id: session_id, archived_seq: archived_seq}, acc
      when is_binary(session_id) and session_id != "" and
             is_integer(archived_seq) and archived_seq >= 0 ->
        Map.update(
          acc,
          session_id,
          %{session_id: session_id, archived_seq: archived_seq},
          fn current ->
            %{current | archived_seq: max(current.archived_seq, archived_seq)}
          end
        )

      _invalid_update, _acc ->
        raise ArgumentError, "invalid session progress update"
    end)
    |> Map.values()
  end

  defp put_progress_chunk([]), do: :ok

  defp put_progress_chunk(progress_updates) when is_list(progress_updates) do
    {values_sql, params} =
      progress_updates
      |> Enum.with_index()
      |> Enum.map_reduce([], fn {%{
                                   session_id: session_id,
                                   archived_seq: archived_seq
                                 }, index},
                                acc ->
        offset = index * 2
        placeholder = "($#{offset + 1}::text, $#{offset + 2}::bigint)"
        {placeholder, acc ++ [session_id, archived_seq]}
      end)

    sql = """
    UPDATE sessions AS s
    SET archived_seq = GREATEST(s.archived_seq, v.archived_seq)
    FROM (VALUES #{Enum.join(values_sql, ", ")}) AS v(id, archived_seq)
    WHERE s.id = v.id
    RETURNING s.id
    """

    case Repo.query(sql, params) do
      {:ok, %{rows: rows}} when length(rows) == length(progress_updates) ->
        :ok

      {:ok, _result} ->
        {:error, :session_not_found}

      {:error, _reason} ->
        {:error, :session_catalog_unavailable}
    end
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

  defp apply_cursor(query, nil), do: query

  defp apply_cursor(query, cursor) when is_binary(cursor) and cursor != "" do
    where(query, [s], s.id > ^cursor)
  end

  defp apply_tenant_filter(query, nil), do: query

  defp apply_tenant_filter(query, tenant_id) when is_binary(tenant_id) and tenant_id != "" do
    where(query, [s], s.tenant_id == ^tenant_id)
  end

  defp apply_tenant_filter(query, _invalid_filter), do: where(query, [s], false)

  defp apply_owner_principal_filters(query, nil), do: query

  defp apply_owner_principal_filters(query, owner_principal_ids)
       when is_list(owner_principal_ids) do
    owner_principal_ids =
      owner_principal_ids
      |> Enum.uniq()
      |> Enum.reject(&(&1 == ""))

    case owner_principal_ids do
      [] ->
        where(query, [s], false)

      _other ->
        where(query, [s], s.creator_id in ^owner_principal_ids)
    end
  end

  defp apply_owner_principal_filters(query, _invalid_filter), do: where(query, [s], false)

  defp apply_metadata_filters(query, metadata_filters) when map_size(metadata_filters) == 0,
    do: query

  defp apply_metadata_filters(query, metadata_filters) do
    Enum.reduce(metadata_filters, query, fn {key, value}, acc ->
      where(acc, [s], fragment("? @> ?", s.metadata, type(^%{key => value}, :map)))
    end)
  end

  defp build_session_page(rows, limit) when is_list(rows) and is_integer(limit) and limit > 0 do
    {:ok,
     %{
       sessions: Enum.map(rows, &to_session_row/1),
       next_cursor: next_cursor(rows, limit)
     }}
  end

  defp next_cursor(rows, limit) when is_list(rows) and length(rows) == limit do
    %SessionRecord{id: id} = List.last(rows)
    id
  end

  defp next_cursor(_rows, _limit), do: nil

  defp to_session_row(%SessionRecord{} = session) do
    creator_principal =
      creator_principal!(session.tenant_id, session.creator_id, session.creator_type)

    %{
      id: session.id,
      title: session.title,
      tenant_id: session.tenant_id,
      creator_principal: creator_principal,
      metadata: session.metadata || %{},
      created_at: DateTime.to_iso8601(session.created_at)
    }
  end

  defp row_to_header(
         session_id,
         %SessionRecord{
           id: session_id,
           title: title,
           tenant_id: tenant_id,
           creator_id: creator_id,
           creator_type: creator_type,
           metadata: metadata,
           created_at: created_at
         }
       )
       when is_binary(tenant_id) and tenant_id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) do
    with {:ok, timestamp} <- session_created_at(created_at),
         {:ok, creator_principal} <- creator_principal(tenant_id, creator_id, creator_type) do
      try do
        {:ok,
         Header.new(session_id,
           title: title,
           tenant_id: tenant_id,
           creator_principal: creator_principal,
           metadata: metadata,
           timestamp: timestamp
         )}
      rescue
        ArgumentError -> {:error, :session_catalog_unavailable}
      end
    end
  end

  defp row_to_header(_session_id, _row), do: {:error, :session_catalog_unavailable}

  defp session_created_at(%DateTime{} = created_at), do: {:ok, DateTime.to_naive(created_at)}
  defp session_created_at(%NaiveDateTime{} = created_at), do: {:ok, created_at}

  defp session_created_at(created_at) when is_binary(created_at) do
    case DateTime.from_iso8601(created_at) do
      {:ok, datetime, _offset} ->
        {:ok, DateTime.to_naive(datetime)}

      _ ->
        case NaiveDateTime.from_iso8601(created_at) do
          {:ok, datetime} -> {:ok, datetime}
          _ -> {:error, :session_catalog_unavailable}
        end
    end
  end

  defp session_created_at(_created_at), do: {:error, :session_catalog_unavailable}

  defp creator_identity(nil), do: {nil, nil}

  defp creator_identity(%Principal{id: id, type: type})
       when is_binary(id) and id != "" and type in [:user, :agent, :service] do
    {id, Atom.to_string(type)}
  end

  defp creator_identity(_creator_principal), do: raise(ArgumentError, "invalid session creator")

  defp creator_principal(tenant_id, nil, nil)
       when is_binary(tenant_id) and tenant_id != "" do
    {:ok, nil}
  end

  defp creator_principal(tenant_id, creator_id, creator_type)
       when is_binary(tenant_id) and tenant_id != "" and is_binary(creator_id) and
              creator_id != "" and
              is_binary(creator_type) and creator_type != "" do
    case Principal.new(tenant_id, creator_id, creator_type!(creator_type)) do
      {:ok, principal} -> {:ok, principal}
      {:error, :invalid_principal} -> {:error, :session_catalog_unavailable}
    end
  rescue
    ArgumentError -> {:error, :session_catalog_unavailable}
  end

  defp creator_principal(_tenant_id, _creator_id, _creator_type),
    do: {:error, :session_catalog_unavailable}

  defp creator_principal!(tenant_id, creator_id, creator_type) do
    case creator_principal(tenant_id, creator_id, creator_type) do
      {:ok, creator_principal} -> creator_principal
      {:error, :session_catalog_unavailable} -> raise ArgumentError, "invalid session creator"
    end
  end

  defp creator_type!("user"), do: :user
  defp creator_type!("agent"), do: :agent
  defp creator_type!("service"), do: :service
  defp creator_type!("svc"), do: :service

  defp creator_type!(type),
    do: raise(ArgumentError, "invalid session creator_type: #{inspect(type)}")
end
