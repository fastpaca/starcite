defmodule Starcite.WritePath do
  @moduledoc """
  Write path for session creation and append/ack operations.
  """

  alias Starcite.DataPlane.{RaftAccess, ReplicaRouter}

  @timeout Application.compile_env(:starcite, :raft_command_timeout_ms, 2_000)

  @spec create_session(keyword()) :: {:ok, map()} | {:error, term()}
  def create_session(opts \\ []) when is_list(opts) do
    id =
      case Keyword.get(opts, :id) do
        nil -> generate_session_id()
        value -> value
      end

    title = Keyword.get(opts, :title)
    creator_principal = Keyword.get(opts, :creator_principal)
    metadata = Keyword.get(opts, :metadata, %{})
    group = RaftAccess.group_for_session(id)

    result =
      case RaftAccess.local_server_for_group(group) do
        {:ok, server_id} ->
          process_command(server_id, {:create_session, id, title, creator_principal, metadata})

        :error ->
          call_remote(group, :create_session_local, [id, title, creator_principal, metadata])
      end

    case result do
      {:ok, session} = ok ->
        _ = maybe_index_session(session, creator_principal)
        ok

      other ->
        other
    end
  end

  @doc false
  def create_session_local(id, title, creator_principal, metadata)
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             (is_struct(creator_principal, Starcite.Auth.Principal) or
                is_nil(creator_principal)) and
             is_map(metadata) do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command(server_id, {:create_session, id, title, creator_principal, metadata})
    end
  end

  def create_session_local(_id, _title, _creator_principal, _metadata),
    do: {:error, :invalid_session}

  @spec append_event(String.t(), map()) ::
          {:ok, %{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}}
          | {:error, term()}
          | {:timeout, term()}
  def append_event(id, event) when is_binary(id) and id != "" and is_map(event) do
    group = RaftAccess.group_for_session(id)

    case RaftAccess.local_server_for_group(group) do
      {:ok, server_id} ->
        process_command(server_id, {:append_event, id, event, nil})

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
        process_command(server_id, {:append_event, id, event, expected_seq})

      :error ->
        call_remote(group, :append_event_local, [id, event, opts])
    end
  end

  def append_event(_id, _event, _opts), do: {:error, :invalid_event}

  @doc false
  def append_event_local(id, event)
      when is_binary(id) and id != "" and is_map(event) do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command(server_id, {:append_event, id, event, nil})
    end
  end

  @doc false
  def append_event_local(id, event, opts) when is_binary(id) and id != "" and is_map(event) do
    expected_seq = expected_seq_from_opts(opts)

    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command(server_id, {:append_event, id, event, expected_seq})
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
        process_command(server_id, {:append_events, id, events, opts})

      :error ->
        call_remote(group, :append_events_local, [id, events, opts])
    end
  end

  def append_events(_id, _events, _opts), do: {:error, :invalid_event}

  @doc false
  def append_events_local(id, events, opts \\ [])
      when is_binary(id) and id != "" and is_list(events) and events != [] and is_list(opts) do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command(server_id, {:append_events, id, events, opts})
    end
  end

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def ack_archived(id, upto_seq) when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    group = RaftAccess.group_for_session(id)

    case RaftAccess.local_server_for_group(group) do
      {:ok, server_id} ->
        process_command(server_id, {:ack_archived, id, upto_seq})

      :error ->
        call_remote(group, :ack_archived_local, [id, upto_seq])
    end
  end

  @doc false
  def ack_archived_local(id, upto_seq)
      when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      process_command(server_id, {:ack_archived, id, upto_seq})
    end
  end

  defp maybe_index_session(
         %{
           id: id,
           title: title,
           metadata: metadata,
           created_at: created_at
         },
         creator_principal
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              (is_struct(creator_principal, Starcite.Auth.Principal) or
                 is_nil(creator_principal)) and
              is_map(metadata) do
    row = %{
      id: id,
      title: title,
      creator_principal: creator_principal,
      metadata: metadata,
      created_at: parse_utc_datetime!(created_at)
    }

    case Starcite.Archive.Store.upsert_session(row) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp maybe_index_session(_session, _creator_principal), do: :ok

  defp call_remote(group_id, fun, args)
       when is_integer(group_id) and group_id >= 0 and is_atom(fun) and is_list(args) do
    ReplicaRouter.call_on_replica(
      group_id,
      __MODULE__,
      fun,
      args,
      __MODULE__,
      fun,
      args,
      prefer_leader: false
    )
  end

  defp process_command(server_id, command) when is_atom(server_id) do
    case :ra.process_command({server_id, Node.self()}, command, @timeout) do
      {:ok, {:reply, {:ok, reply}}, _leader} -> {:ok, reply}
      {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
      {:timeout, leader} -> {:timeout, leader}
      {:error, reason} -> {:error, reason}
    end
  end

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
