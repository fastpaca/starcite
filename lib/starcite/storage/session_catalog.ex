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
  alias Starcite.Session.Projections
  alias Starcite.Storage.SessionRecord

  @progress_batch_size 5_000

  @type session_query :: %{
          required(:limit) => pos_integer(),
          required(:cursor) => String.t() | nil,
          required(:archived) => :active | :archived | :all,
          required(:metadata) => map(),
          optional(:owner_principal_ids) => [String.t()],
          optional(:session_ids) => [String.t()],
          optional(:tenant_id) => String.t()
        }

  @type session_entry :: %{
          required(:id) => String.t(),
          required(:title) => String.t() | nil,
          required(:tenant_id) => String.t(),
          required(:creator_principal) => Principal.t() | nil,
          required(:metadata) => map(),
          required(:created_at) => String.t(),
          required(:updated_at) => String.t(),
          required(:version) => pos_integer(),
          required(:archived) => boolean()
        }

  @type session_page :: %{
          required(:sessions) => [session_entry()],
          required(:next_cursor) => String.t() | nil
        }

  @type progress_update :: %{
          required(:session_id) => String.t(),
          required(:archived_seq) => non_neg_integer()
        }

  @type archive_context :: %{
          required(:tenant_id) => String.t(),
          required(:archived_seq) => non_neg_integer()
        }

  @type header_update :: %{
          optional(:title) => String.t() | nil,
          optional(:metadata) => map(),
          optional(:expected_version) => pos_integer()
        }

  @type archive_update_result :: %{
          required(:session) => session_entry(),
          required(:changed) => boolean()
        }

  @spec persist_created(Header.t()) :: :ok | {:error, term()}
  def persist_created(%Header{
        id: id,
        title: title,
        creator_principal: creator_principal,
        tenant_id: tenant_id,
        metadata: metadata,
        created_at: created_at,
        updated_at: updated_at,
        version: version
      })
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_binary(tenant_id) and tenant_id != "" and is_map(metadata) and
             is_integer(version) and version > 0 do
    {creator_id, creator_type} = creator_identity(creator_principal)
    persisted_at = created_at |> DateTime.from_naive!("Etc/UTC") |> DateTime.truncate(:second)

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
            projection_state: Projections.to_map(Projections.new()),
            projection_version: 0,
            created_at: persisted_at,
            updated_at:
              updated_at |> DateTime.from_naive!("Etc/UTC") |> DateTime.truncate(:microsecond),
            version: version
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

  @spec update_header(String.t(), header_update()) :: {:ok, Header.t()} | {:error, term()}
  def update_header(session_id, attrs)
      when is_binary(session_id) and session_id != "" and is_map(attrs) do
    title = Map.get(attrs, :title, :unchanged)
    metadata = Map.get(attrs, :metadata, :unchanged)
    expected_version = Map.get(attrs, :expected_version)

    cond do
      title == :unchanged and metadata == :unchanged ->
        {:error, :invalid_session}

      title != :unchanged and not (is_binary(title) or is_nil(title)) ->
        {:error, :invalid_session}

      metadata != :unchanged and not is_map(metadata) ->
        {:error, :invalid_session}

      title == :unchanged and metadata == %{} ->
        {:error, :invalid_session}

      not is_nil(expected_version) and not (is_integer(expected_version) and expected_version > 0) ->
        {:error, :invalid_session}

      true ->
        with {:ok, row} <- do_update_header(session_id, title, metadata, expected_version) do
          row_to_header(session_id, row)
        end
    end
  end

  def update_header(_session_id, _attrs), do: {:error, :invalid_session}

  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, row} <- get_record(session_id),
         {:ok, header} <- row_to_header(session_id, row),
         archived_seq when is_integer(archived_seq) and archived_seq >= 0 <- row.archived_seq,
         projection_version
         when is_integer(projection_version) and projection_version >= 0 <- row.projection_version,
         {:ok, session} <-
           Session.hydrate(header, archived_seq, row.projection_state, projection_version) do
      {:ok, session}
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

  @spec get_session_entry(String.t()) :: {:ok, session_entry()} | {:error, term()}
  def get_session_entry(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, row} <- get_record(session_id) do
      {:ok, to_session_row(row)}
    end
  end

  def get_session_entry(_session_id), do: {:error, :invalid_session_id}

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

  @spec get_archive_context(String.t()) :: {:ok, archive_context()} | {:error, term()}
  def get_archive_context(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, row} <- get_record(session_id),
         tenant_id when is_binary(tenant_id) and tenant_id != "" <- row.tenant_id,
         archived_seq when is_integer(archived_seq) and archived_seq >= 0 <- row.archived_seq do
      {:ok, %{tenant_id: tenant_id, archived_seq: archived_seq}}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :session_catalog_unavailable}
    end
  end

  def get_archive_context(_session_id), do: {:error, :invalid_session_id}

  @spec get_projection_version(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def get_projection_version(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, row} <- get_record(session_id),
         projection_version
         when is_integer(projection_version) and projection_version >= 0 <- row.projection_version do
      {:ok, projection_version}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :session_catalog_unavailable}
    end
  end

  def get_projection_version(_session_id), do: {:error, :invalid_session_id}

  @spec archive_session(String.t()) :: {:ok, archive_update_result()} | {:error, term()}
  def archive_session(session_id) when is_binary(session_id) and session_id != "" do
    set_archive_state(session_id, true)
  end

  def archive_session(_session_id), do: {:error, :invalid_session_id}

  @spec unarchive_session(String.t()) :: {:ok, archive_update_result()} | {:error, term()}
  def unarchive_session(session_id) when is_binary(session_id) and session_id != "" do
    set_archive_state(session_id, false)
  end

  def unarchive_session(_session_id), do: {:error, :invalid_session_id}

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

  @spec put_projection_state(String.t(), Projections.t(), non_neg_integer()) ::
          :ok | {:error, term()}
  def put_projection_state(session_id, %Projections{} = projections, projection_version)
      when is_binary(session_id) and session_id != "" and is_integer(projection_version) and
             projection_version >= 0 do
    projection_state = Projections.to_map(projections)

    case Repo.transaction(fn ->
           with {:ok, row} <- get_record_for_update(session_id) do
             current_version = row.projection_version || 0

             cond do
               projection_version > current_version ->
                 persist_projection_state_row(row, projection_state, projection_version)

               projection_version == current_version and row.projection_state == projection_state ->
                 :ok

               projection_version == current_version ->
                 Repo.rollback(
                   {:projection_version_conflict, projection_version, current_version}
                 )

               true ->
                 Repo.rollback({:stale_projection_version, projection_version, current_version})
             end
           else
             {:error, reason} -> Repo.rollback(reason)
           end
         end) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

  def put_projection_state(_session_id, _projections, _projection_version),
    do: {:error, :invalid_projection_item}

  @spec list_sessions(session_query()) :: {:ok, session_page()} | {:error, term()}
  def list_sessions(
        %{limit: limit, cursor: cursor, archived: archived, metadata: metadata} = query_opts
      )
      when is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    session_ids = Map.get(query_opts, :session_ids)

    query =
      SessionRecord
      |> then(fn query ->
        case session_ids do
          nil -> query
          session_ids when is_list(session_ids) -> where(query, [s], s.id in ^session_ids)
        end
      end)
      |> then(fn query ->
        if is_nil(cursor), do: query, else: where(query, [s], s.id > ^cursor)
      end)
      |> then(fn query ->
        case archived do
          :active -> where(query, [s], s.archived == false)
          :archived -> where(query, [s], s.archived == true)
          :all -> query
        end
      end)
      |> then(fn query ->
        case Map.get(query_opts, :tenant_id) do
          nil ->
            query

          tenant_id when is_binary(tenant_id) and tenant_id != "" ->
            where(query, [s], s.tenant_id == ^tenant_id)
        end
      end)
      |> then(fn query ->
        case Map.get(query_opts, :owner_principal_ids) do
          nil ->
            query

          owner_principal_ids ->
            owner_principal_ids =
              owner_principal_ids |> Enum.uniq() |> Enum.reject(&(&1 == ""))

            case owner_principal_ids do
              [] -> where(query, [s], false)
              _other -> where(query, [s], s.creator_id in ^owner_principal_ids)
            end
        end
      end)
      |> then(fn query ->
        Enum.reduce(metadata, query, fn {key, value}, acc ->
          where(acc, [s], fragment("? @> ?", s.metadata, type(^%{key => value}, :map)))
        end)
      end)
      |> order_by([s], asc: s.id)
      |> limit(^limit)

    fetch_session_page(query, limit)
  end

  defp get_record(session_id) when is_binary(session_id) and session_id != "" do
    case Repo.get(SessionRecord, session_id) do
      %SessionRecord{} = row -> {:ok, row}
      nil -> {:error, :session_not_found}
    end
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

  defp do_update_header(session_id, title, metadata, expected_version)
       when is_binary(session_id) and session_id != "" and
              (is_binary(title) or is_nil(title) or title == :unchanged) and
              (is_map(metadata) or metadata == :unchanged) and
              (is_nil(expected_version) or (is_integer(expected_version) and expected_version > 0)) do
    case Repo.transaction(fn ->
           with {:ok, row} <- get_record_for_update(session_id),
                :ok <- guard_expected_version(row, expected_version) do
             changes = %{
               updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
               version: row.version + 1
             }

             changes = if title == :unchanged, do: changes, else: Map.put(changes, :title, title)

             changes =
               if metadata == :unchanged do
                 changes
               else
                 Map.put(changes, :metadata, Map.merge(row.metadata || %{}, metadata))
               end

             case Repo.update(Ecto.Changeset.change(row, changes)) do
               {:ok, %SessionRecord{} = updated_row} -> updated_row
               {:error, _changeset} -> Repo.rollback(:session_catalog_unavailable)
             end
           else
             {:error, reason} -> Repo.rollback(reason)
           end
         end) do
      {:ok, %SessionRecord{} = row} -> {:ok, row}
      {:error, reason} -> {:error, reason}
    end
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

  defp get_record_for_update(session_id) when is_binary(session_id) and session_id != "" do
    query =
      from(session in SessionRecord,
        where: session.id == ^session_id,
        lock: "FOR UPDATE"
      )

    case Repo.one(query) do
      %SessionRecord{} = row -> {:ok, row}
      nil -> {:error, :session_not_found}
    end
  end

  defp get_record_for_update(_session_id), do: {:error, :session_catalog_unavailable}

  defp persist_projection_state_row(
         %SessionRecord{} = row,
         projection_state,
         projection_version
       )
       when is_map(projection_state) and is_integer(projection_version) and
              projection_version >= 0 do
    case Repo.update(
           Ecto.Changeset.change(row,
             projection_state: projection_state,
             projection_version: projection_version
           )
         ) do
      {:ok, %SessionRecord{}} -> :ok
      {:error, _changeset} -> Repo.rollback(:session_catalog_unavailable)
    end
  end

  defp guard_expected_version(%SessionRecord{}, nil), do: :ok

  defp guard_expected_version(
         %SessionRecord{version: current_version},
         expected_version
       ) do
    if current_version == expected_version do
      :ok
    else
      {:error, {:expected_version_conflict, expected_version, current_version}}
    end
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

  defp build_session_page(rows, limit) when is_list(rows) and is_integer(limit) and limit > 0 do
    {:ok,
     %{
       sessions: Enum.map(rows, &to_session_row/1),
       next_cursor: next_cursor(rows, limit)
     }}
  end

  defp fetch_session_page(query, limit) do
    rows = Repo.all(query)
    build_session_page(rows, limit)
  rescue
    _ -> {:error, :session_catalog_unavailable}
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
      created_at: updated_at_to_iso8601(session.created_at),
      updated_at: updated_at_to_iso8601(session.updated_at || session.created_at),
      version: session.version,
      archived: session.archived
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
           created_at: created_at,
           updated_at: updated_at,
           version: version
         }
       )
       when is_binary(tenant_id) and tenant_id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) and is_integer(version) and version > 0 do
    with {:ok, timestamp} <- session_created_at(created_at),
         {:ok, updated_at} <- session_updated_at(updated_at, created_at),
         {:ok, creator_principal} <- creator_principal(tenant_id, creator_id, creator_type) do
      try do
        {:ok,
         Header.new(session_id,
           title: title,
           tenant_id: tenant_id,
           creator_principal: creator_principal,
           metadata: metadata,
           timestamp: timestamp,
           updated_at: updated_at,
           version: version
         )}
      rescue
        ArgumentError -> {:error, :session_catalog_unavailable}
      end
    end
  end

  defp row_to_header(_session_id, _row), do: {:error, :session_catalog_unavailable}

  defp set_archive_state(session_id, archived)
       when is_binary(session_id) and session_id != "" and is_boolean(archived) do
    updated_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    case Repo.update_all(
           from(s in SessionRecord, where: s.id == ^session_id and s.archived != ^archived),
           set: [archived: archived, updated_at: updated_at]
         ) do
      {1, _} ->
        with {:ok, updated_row} <- get_record(session_id) do
          {:ok, %{session: to_session_row(updated_row), changed: true}}
        end

      {0, _} ->
        with {:ok, row} <- get_record(session_id) do
          {:ok, %{session: to_session_row(row), changed: false}}
        end
    end
  rescue
    _ -> {:error, :session_catalog_unavailable}
  end

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

  defp session_updated_at(nil, created_at), do: session_created_at(created_at)

  defp session_updated_at(%DateTime{} = updated_at, _created_at),
    do: {:ok, DateTime.to_naive(updated_at)}

  defp session_updated_at(%NaiveDateTime{} = updated_at, _created_at), do: {:ok, updated_at}

  defp session_updated_at(updated_at, _created_at) when is_binary(updated_at) do
    case DateTime.from_iso8601(updated_at) do
      {:ok, datetime, _offset} ->
        {:ok, DateTime.to_naive(datetime)}

      _ ->
        case NaiveDateTime.from_iso8601(updated_at) do
          {:ok, datetime} -> {:ok, datetime}
          _ -> {:error, :session_catalog_unavailable}
        end
    end
  end

  defp session_updated_at(_updated_at, _created_at), do: {:error, :session_catalog_unavailable}

  defp updated_at_to_iso8601(%DateTime{} = updated_at), do: DateTime.to_iso8601(updated_at)

  defp updated_at_to_iso8601(%NaiveDateTime{} = updated_at) do
    updated_at
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

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
