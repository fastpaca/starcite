defmodule Starcite.WritePath do
  @moduledoc """
  Write path for session creation and append/ack operations.
  """

  alias Starcite.Archive.Store

  alias Starcite.DataPlane.{
    RaftAccess,
    RaftBootstrap,
    RaftPipelineClient,
    ReplicaRouter,
    SessionStore
  }

  alias Starcite.Auth.Principal
  alias Starcite.Observability.Telemetry
  alias Starcite.Session
  alias Starcite.Session.RuntimeSnapshot

  @timeout Application.compile_env(:starcite, :raft_command_timeout_ms, 2_000)

  @spec create_session(keyword()) :: {:ok, map()} | {:error, term()}
  def create_session(opts \\ []) when is_list(opts) do
    id =
      case Keyword.get(opts, :id) do
        nil -> generate_session_id()
        value -> value
      end

    title = Keyword.get(opts, :title)
    metadata = Keyword.get(opts, :metadata, %{})

    if is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and is_map(metadata) do
      dispatch_create_session(
        id,
        title,
        Keyword.get(opts, :creator_principal),
        Keyword.get(opts, :tenant_id),
        metadata
      )
    else
      {:error, :invalid_session}
    end
  end

  @doc false
  def create_session_local(id, title, creator_principal, tenant_id, metadata)
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
             is_map(metadata) do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command_with_leader_retry(
        server_id,
        {:create_session, id, title, creator_principal, tenant_id, metadata}
      )
    end
  end

  def create_session_local(_id, _title, _creator_principal, _tenant_id, _metadata),
    do: {:error, :invalid_session}

  defp dispatch_create_session(
         id,
         title,
         %Principal{tenant_id: tenant_id} = creator_principal,
         tenant_id,
         metadata
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    do_create_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp dispatch_create_session(
         id,
         title,
         %Principal{tenant_id: tenant_id} = creator_principal,
         nil,
         metadata
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    do_create_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp dispatch_create_session(id, title, nil, tenant_id, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    {:ok, creator_principal} = Principal.new(tenant_id, "service", :service)
    do_create_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp dispatch_create_session(id, title, nil, nil, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) do
    {:ok, creator_principal} = Principal.new("service", "service", :service)
    do_create_session(id, title, creator_principal, "service", metadata)
  end

  defp dispatch_create_session(_id, _title, _creator_principal, _tenant_id, _metadata),
    do: {:error, :invalid_session}

  defp do_create_session(id, title, creator_principal, tenant_id, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
              is_map(metadata) do
    group = RaftAccess.group_for_session(id)

    result =
      case RaftAccess.local_server_for_group(group) do
        {:ok, server_id} ->
          process_command_with_leader_retry(
            server_id,
            {:create_session, id, title, creator_principal, tenant_id, metadata}
          )

        :error ->
          call_remote(
            group,
            :create_session_local,
            [id, title, creator_principal, tenant_id, metadata]
          )
      end

    case result do
      {:ok, session} = ok ->
        _ = maybe_index_session(session, creator_principal, tenant_id)
        # Warm local session cache so immediate same-node reads stay on the RAM path.
        _ = maybe_cache_session(id, title, creator_principal, tenant_id, metadata)
        :ok = Telemetry.session_create(id, tenant_id)
        ok

      other ->
        other
    end
  end

  @spec append_event(String.t(), map()) ::
          {:ok, %{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}}
          | {:error, term()}
          | {:timeout, term()}
  def append_event(id, event) when is_binary(id) and id != "" and is_map(event) do
    group = RaftAccess.group_for_session(id)

    case RaftAccess.local_server_for_group(group) do
      {:ok, server_id} ->
        process_append_with_hydrate_retry(server_id, id, {:append_event, id, event, nil})

      :error ->
        call_remote(group, :append_event_local, [id, event])
    end
  end

  def append_event(_id, _event), do: {:error, :invalid_event}

  @spec append_event(String.t(), map(), keyword()) ::
          {:ok, %{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}}
          | {:error, term()}
          | {:timeout, term()}
  def append_event(id, event, opts) when is_binary(id) and id != "" and is_map(event) do
    group = RaftAccess.group_for_session(id)
    expected_seq = expected_seq_from_opts(opts)

    case RaftAccess.local_server_for_group(group) do
      {:ok, server_id} ->
        process_append_with_hydrate_retry(
          server_id,
          id,
          {:append_event, id, event, expected_seq}
        )

      :error ->
        call_remote(
          group,
          :append_event_local,
          [id, event, opts]
        )
    end
  end

  def append_event(_id, _event, _opts), do: {:error, :invalid_event}

  @doc false
  def append_event_local(id, event)
      when is_binary(id) and id != "" and is_map(event) do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_append_with_hydrate_retry(server_id, id, {:append_event, id, event, nil})
    end
  end

  @doc false
  def append_event_local(id, event, opts) when is_binary(id) and id != "" and is_map(event) do
    expected_seq = expected_seq_from_opts(opts)

    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_append_with_hydrate_retry(
        server_id,
        id,
        {:append_event, id, event, expected_seq}
      )
    end
  end

  @spec append_events(String.t(), [map()], keyword()) ::
          {:ok,
           %{
             results: [%{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}],
             last_seq: non_neg_integer()
           }}
          | {:error, term()}
          | {:timeout, term()}
  def append_events(id, events, opts \\ [])

  def append_events(id, events, opts)
      when is_binary(id) and id != "" and is_list(events) and events != [] and is_list(opts) do
    group = RaftAccess.group_for_session(id)

    case RaftAccess.local_server_for_group(group) do
      {:ok, server_id} ->
        process_append_with_hydrate_retry(server_id, id, {:append_events, id, events, opts})

      :error ->
        call_remote(group, :append_events_local, [id, events, opts])
    end
  end

  def append_events(_id, _events, _opts), do: {:error, :invalid_event}

  @doc false
  def append_events_local(id, events, opts \\ [])
      when is_binary(id) and id != "" and is_list(events) and events != [] and is_list(opts) do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_append_with_hydrate_retry(server_id, id, {:append_events, id, events, opts})
    end
  end

  @type ack_archived_entry :: {String.t(), non_neg_integer()}
  @type ack_archived_applied :: %{
          session_id: String.t(),
          archived_seq: non_neg_integer(),
          trimmed: non_neg_integer()
        }
  @type ack_archived_failed :: %{session_id: String.t(), reason: term()}
  @type ack_archived_result :: %{
          applied: [ack_archived_applied()],
          failed: [ack_archived_failed()]
        }

  @spec ack_archived([ack_archived_entry()]) ::
          {:ok, ack_archived_result()} | {:error, term()} | {:timeout, term()}
  def ack_archived(entries) when is_list(entries) and entries != [] do
    with {:ok, grouped_entries} <- normalize_ack_archived_entries(entries) do
      result =
        Enum.reduce(grouped_entries, %{applied: [], failed: []}, fn {group, group_entries}, acc ->
          dispatch_result =
            case RaftAccess.local_server_for_group(group) do
              {:ok, server_id} ->
                process_command_with_leader_retry(server_id, {:ack_archived, group_entries})

              :error ->
                call_remote(group, :ack_archived_local, [group_entries])
            end

          merge_ack_archived_result(acc, group_entries, dispatch_result)
        end)

      {:ok, %{applied: Enum.reverse(result.applied), failed: Enum.reverse(result.failed)}}
    end
  end

  def ack_archived(_entries), do: {:error, :invalid_archive_ack}

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, ack_archived_result()} | {:error, term()} | {:timeout, term()}
  def ack_archived(id, upto_seq) when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    ack_archived([{id, upto_seq}])
  end

  def ack_archived(_id, _upto_seq), do: {:error, :invalid_archive_ack}

  @doc false
  def ack_archived_local(entries) when is_list(entries) and entries != [] do
    with [{session_id, _upto_seq} | _rest] <- entries,
         {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(session_id) do
      process_command_with_leader_retry(server_id, {:ack_archived, entries})
    else
      _ -> {:error, :invalid_archive_ack}
    end
  end

  @doc false
  def ack_archived_local(id, upto_seq)
      when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    ack_archived_local([{id, upto_seq}])
  end

  def ack_archived_local(_id, _upto_seq), do: {:error, :invalid_archive_ack}

  defp maybe_cache_session(id, title, creator_principal, tenant_id, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
              is_map(metadata) do
    session =
      Session.new(id,
        title: title,
        creator_principal: creator_principal,
        tenant_id: tenant_id,
        metadata: metadata
      )

    SessionStore.put_session(session)
  end

  defp maybe_cache_session(_id, _title, _creator_principal, _tenant_id, _metadata), do: :ok

  defp maybe_index_session(
         %{
           id: id,
           title: title,
           metadata: metadata,
           created_at: created_at
         },
         creator_principal,
         tenant_id
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
              is_map(metadata) do
    snapshot = %RuntimeSnapshot{
      archived_seq: 0,
      tail_keep: Application.get_env(:starcite, :tail_keep, 1_000),
      producer_max_entries: Application.get_env(:starcite, :producer_max_entries, 10_000)
    }

    row = %{
      id: id,
      title: title,
      creator_principal: creator_principal,
      tenant_id: tenant_id,
      metadata: RuntimeSnapshot.put_in_metadata(metadata, snapshot),
      created_at: parse_utc_datetime!(created_at)
    }

    case Store.upsert_session(row) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp maybe_index_session(_session, _creator_principal, _tenant_id), do: :ok

  defp normalize_ack_archived_entries(entries) when is_list(entries) do
    case Enum.reduce_while(entries, %{}, fn
           {session_id, upto_seq}, acc
           when is_binary(session_id) and session_id != "" and is_integer(upto_seq) and
                  upto_seq >= 0 ->
             {:cont, Map.update(acc, session_id, upto_seq, &max(&1, upto_seq))}

           _entry, _acc ->
             {:halt, :error}
         end) do
      deduped when is_map(deduped) and map_size(deduped) > 0 ->
        grouped_entries =
          Enum.reduce(deduped, %{}, fn {session_id, upto_seq}, acc ->
            group = RaftAccess.group_for_session(session_id)
            Map.update(acc, group, [{session_id, upto_seq}], &[{session_id, upto_seq} | &1])
          end)
          |> Enum.map(fn {group, group_entries} -> {group, Enum.reverse(group_entries)} end)

        {:ok, grouped_entries}

      _ ->
        {:error, :invalid_archive_ack}
    end
  end

  defp merge_ack_archived_result(
         %{applied: applied_acc, failed: failed_acc},
         group_entries,
         {:ok, %{applied: applied, failed: failed}}
       )
       when is_list(group_entries) and is_list(applied) and is_list(failed) do
    %{applied: Enum.reverse(applied, applied_acc), failed: Enum.reverse(failed, failed_acc)}
  end

  defp merge_ack_archived_result(
         %{applied: applied_acc, failed: failed_acc},
         group_entries,
         error
       )
       when is_list(group_entries) do
    reason =
      case error do
        {:error, failure} -> failure
        {:timeout, failure} -> {:timeout, failure}
        other -> {:invalid_archive_ack_reply, other}
      end

    failed =
      Enum.map(group_entries, fn {session_id, _upto_seq} ->
        %{session_id: session_id, reason: reason}
      end)

    %{applied: applied_acc, failed: Enum.reverse(failed, failed_acc)}
  end

  defp call_remote(group_id, fun, args)
       when is_integer(group_id) and group_id >= 0 and is_atom(fun) and is_list(args) do
    route_opts = [prefer_leader: false]

    ReplicaRouter.call_on_replica(
      group_id,
      __MODULE__,
      fun,
      args,
      __MODULE__,
      fun,
      args,
      route_opts
    )
  end

  defp process_command_with_leader_retry(server_id, command) when is_atom(server_id) do
    self_node = Node.self()
    local_result = process_command_on_node(server_id, self_node, command)

    {final_result, outcome} =
      case local_result do
        {:timeout, {^server_id, leader_node}}
        when is_atom(leader_node) and not is_nil(leader_node) ->
          if leader_node == self_node do
            {local_result, :local_timeout}
          else
            retry_result = process_command_on_node(server_id, leader_node, command)
            {retry_result, classify_leader_retry_outcome(retry_result)}
          end

        _ ->
          {local_result, classify_local_outcome(local_result)}
      end

    :ok = RaftBootstrap.record_write_outcome(outcome)
    final_result
  end

  defp process_append_with_hydrate_retry(server_id, session_id, command)
       when is_atom(server_id) and is_binary(session_id) and session_id != "" do
    case process_command_with_leader_retry(server_id, command) do
      {:error, :session_not_found} ->
        hydrate_and_retry_append(server_id, session_id, command)

      result ->
        result
    end
  end

  defp hydrate_and_retry_append(server_id, session_id, command)
       when is_atom(server_id) and is_binary(session_id) and session_id != "" do
    case load_session_snapshot(session_id) do
      {:ok, snapshot} ->
        tenant_id = snapshot.tenant_id

        with {:ok, hydrate_result} <-
               process_command_with_leader_retry(server_id, hydrate_command(snapshot)),
             true <- hydrate_result in [:hydrated, :already_hot],
             {:ok, _reply} = append_reply <- process_command_with_leader_retry(server_id, command) do
          :ok = Telemetry.session_hydrate(session_id, tenant_id, :ok, hydrate_result)
          append_reply
        else
          false ->
            :ok =
              Telemetry.session_hydrate(
                session_id,
                tenant_id,
                :error,
                :unexpected_hydrate_result
              )

            {:error, :archive_read_unavailable}

          {:error, reason} ->
            normalized = normalize_hydrate_reason(reason)
            :ok = Telemetry.session_hydrate(session_id, tenant_id, :error, normalized)

            case normalized do
              :snapshot_missing -> {:error, :archive_read_unavailable}
              :invalid_snapshot -> {:error, :archive_read_unavailable}
              :unsupported_snapshot_version -> {:error, :archive_read_unavailable}
              _ -> {:error, reason}
            end
        end

      {:error, reason} ->
        normalized = normalize_hydrate_reason(reason)
        :ok = Telemetry.session_hydrate(session_id, "unknown", :error, normalized)

        case normalized do
          :snapshot_missing -> {:error, :archive_read_unavailable}
          :invalid_snapshot -> {:error, :archive_read_unavailable}
          :unsupported_snapshot_version -> {:error, :archive_read_unavailable}
          _ -> {:error, reason}
        end
    end
  end

  defp load_session_snapshot(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, %{sessions: sessions}} when is_list(sessions) <-
           Store.list_sessions_by_ids([session_id], %{limit: 1, cursor: nil, metadata: %{}}),
         {:ok, row} <- select_snapshot_row(sessions),
         {:ok, snapshot} <- decode_snapshot_row(row) do
      {:ok, snapshot}
    end
  end

  defp select_snapshot_row([%{id: session_id} = row | _rest])
       when is_binary(session_id) and session_id != "" do
    {:ok, row}
  end

  defp select_snapshot_row([]), do: {:error, :session_not_found}
  defp select_snapshot_row(_rows), do: {:error, :invalid_snapshot}

  defp decode_snapshot_row(%{
         id: session_id,
         title: title,
         tenant_id: tenant_id,
         creator_principal: creator_principal,
         metadata: metadata,
         created_at: created_at
       })
       when is_binary(session_id) and session_id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    with {:ok, inserted_at} <- parse_inserted_at(created_at),
         {:ok, normalized_creator_principal} <-
           normalize_snapshot_creator_principal(creator_principal),
         {:ok, runtime} <- RuntimeSnapshot.decode_from_metadata(metadata) do
      {:ok,
       %{
         session_id: session_id,
         title: title,
         creator_principal: normalized_creator_principal,
         tenant_id: tenant_id,
         metadata: RuntimeSnapshot.drop_from_metadata(metadata),
         inserted_at: inserted_at,
         last_seq: runtime.archived_seq,
         archived_seq: runtime.archived_seq,
         tail_keep: runtime.tail_keep,
         producer_max_entries: runtime.producer_max_entries
       }}
    end
  end

  defp decode_snapshot_row(_row), do: {:error, :invalid_snapshot}

  defp parse_inserted_at(%DateTime{} = datetime), do: {:ok, DateTime.to_naive(datetime)}
  defp parse_inserted_at(%NaiveDateTime{} = datetime), do: {:ok, datetime}

  defp parse_inserted_at(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        {:ok, DateTime.to_naive(datetime)}

      _ ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, datetime} -> {:ok, datetime}
          _ -> {:error, :invalid_snapshot}
        end
    end
  end

  defp parse_inserted_at(_value), do: {:error, :invalid_snapshot}

  defp normalize_snapshot_creator_principal(%Principal{} = creator_principal),
    do: {:ok, creator_principal}

  defp normalize_snapshot_creator_principal(%{
         "tenant_id" => tenant_id,
         "id" => id,
         "type" => type
       })
       when is_binary(tenant_id) and tenant_id != "" and is_binary(id) and id != "" do
    case normalize_snapshot_principal_type(type) do
      {:ok, normalized_type} -> Principal.new(tenant_id, id, normalized_type)
      {:error, _reason} = error -> error
    end
  end

  defp normalize_snapshot_creator_principal(_creator_principal), do: {:error, :invalid_snapshot}

  defp normalize_snapshot_principal_type(type) when type in [:user, :agent, :service],
    do: {:ok, type}

  defp normalize_snapshot_principal_type("user"), do: {:ok, :user}
  defp normalize_snapshot_principal_type("agent"), do: {:ok, :agent}
  defp normalize_snapshot_principal_type("service"), do: {:ok, :service}
  defp normalize_snapshot_principal_type("svc"), do: {:ok, :service}
  defp normalize_snapshot_principal_type(_type), do: {:error, :invalid_snapshot}

  defp hydrate_command(%{
         session_id: session_id,
         title: title,
         creator_principal: creator_principal,
         tenant_id: tenant_id,
         metadata: metadata,
         inserted_at: inserted_at,
         last_seq: last_seq,
         archived_seq: archived_seq,
         tail_keep: tail_keep,
         producer_max_entries: producer_max_entries
       }) do
    {:hydrate_session, session_id, title, creator_principal, tenant_id, metadata, inserted_at,
     last_seq, archived_seq, tail_keep, producer_max_entries}
  end

  defp normalize_hydrate_reason(reason) when is_atom(reason), do: reason
  defp normalize_hydrate_reason(_reason), do: :unknown

  defp process_command_on_node(server_id, node, command)
       when is_atom(server_id) and is_atom(node) do
    case RaftPipelineClient.command(server_id, node, command, @timeout) do
      {:ok, _reply} = ok ->
        ok

      {:error, _reason} = error ->
        error

      {:timeout, _leader} ->
        process_command_on_node_fallback(server_id, node, command)
    end
  end

  defp process_command_on_node_fallback(server_id, node, command)
       when is_atom(server_id) and is_atom(node) do
    case :ra.process_command({server_id, node}, command, @timeout) do
      {:ok, {:reply, {:ok, reply}}, _leader} -> {:ok, reply}
      {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
      {:timeout, leader} -> {:timeout, leader}
      {:error, reason} -> {:error, reason}
    end
  end

  defp classify_local_outcome({:ok, _reply}), do: :local_ok
  defp classify_local_outcome({:error, _reason}), do: :local_error
  defp classify_local_outcome({:timeout, _leader}), do: :local_timeout

  defp classify_leader_retry_outcome({:ok, _reply}), do: :leader_retry_ok
  defp classify_leader_retry_outcome({:error, _reason}), do: :leader_retry_error
  defp classify_leader_retry_outcome({:timeout, _leader}), do: :leader_retry_timeout

  defp expected_seq_from_opts(opts) when is_list(opts) do
    Keyword.get(opts, :expected_seq)
  end

  defp parse_utc_datetime!(%DateTime{} = datetime), do: datetime

  defp parse_utc_datetime!(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      {:error, reason} -> raise ArgumentError, "invalid datetime: #{inspect(reason)}"
    end
  end

  defp generate_session_id do
    "ses_" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
  end
end
