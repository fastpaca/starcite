defmodule Starcite.WritePath do
  @moduledoc """
  Write path for session creation and append/ack operations.
  """

  alias Starcite.DataPlane.{
    RaftAccess,
    RaftBootstrap,
    RaftPipelineClient,
    ReplicaRouter,
    SessionStore
  }

  alias Starcite.Auth.Principal
  alias Starcite.Session

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
        # Keep archive session catalog in sync for list/get lookups backed by cold storage.
        _ = maybe_index_session(session, creator_principal, tenant_id)
        # Warm local session cache so immediate same-node reads stay on the RAM path.
        _ = maybe_cache_session(id, title, creator_principal, tenant_id, metadata)
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
        process_command_with_leader_retry(server_id, {:append_event, id, event, nil})

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
        process_command_with_leader_retry(server_id, {:append_event, id, event, expected_seq})

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
      process_command_with_leader_retry(server_id, {:append_event, id, event, nil})
    end
  end

  @doc false
  def append_event_local(id, event, opts) when is_binary(id) and id != "" and is_map(event) do
    expected_seq = expected_seq_from_opts(opts)

    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command_with_leader_retry(server_id, {:append_event, id, event, expected_seq})
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
        process_command_with_leader_retry(server_id, {:append_events, id, events, opts})

      :error ->
        call_remote(group, :append_events_local, [id, events, opts])
    end
  end

  def append_events(_id, _events, _opts), do: {:error, :invalid_event}

  @doc false
  def append_events_local(id, events, opts \\ [])
      when is_binary(id) and id != "" and is_list(events) and events != [] and is_list(opts) do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command_with_leader_retry(server_id, {:append_events, id, events, opts})
    end
  end

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def ack_archived(id, upto_seq) when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    group = RaftAccess.group_for_session(id)

    case RaftAccess.local_server_for_group(group) do
      {:ok, server_id} ->
        process_command_with_leader_retry(server_id, {:ack_archived, id, upto_seq})

      :error ->
        call_remote(group, :ack_archived_local, [id, upto_seq])
    end
  end

  @doc false
  def ack_archived_local(id, upto_seq)
      when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command_with_leader_retry(server_id, {:ack_archived, id, upto_seq})
    end
  end

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
    row = %{
      id: id,
      title: title,
      creator_principal: creator_principal,
      tenant_id: tenant_id,
      metadata: metadata,
      created_at: parse_utc_datetime!(created_at)
    }

    case Starcite.Archive.Store.upsert_session(row) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp maybe_index_session(_session, _creator_principal, _tenant_id), do: :ok

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
