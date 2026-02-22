defmodule Starcite.WritePath do
  @moduledoc """
  Write path for session creation and append/ack operations.
  """

  alias Starcite.WritePath.RaftManager
  alias Starcite.WritePath.ReplicaRouter

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
    group = RaftManager.group_for_session(id)

    case ReplicaRouter.call_on_replica(
           group,
           __MODULE__,
           :create_session_local,
           [id, title, creator_principal, metadata],
           __MODULE__,
           :create_session_local,
           [id, title, creator_principal, metadata],
           prefer_leader: true
         ) do
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
    with {:ok, server_id, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.process_command(
             {server_id, Node.self()},
             {:create_session, id, title, creator_principal, metadata},
             @timeout
           ) do
        {:ok, {:reply, {:ok, data}}, _leader} -> {:ok, data}
        {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
        {:timeout, leader} -> {:timeout, leader}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def create_session_local(_id, _title, _creator_principal, _metadata),
    do: {:error, :invalid_session}

  @spec append_event(String.t(), map(), keyword()) ::
          {:ok, %{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}}
          | {:error, term()}
          | {:timeout, term()}
  def append_event(id, event, opts \\ [])

  def append_event(id, event, opts) when is_binary(id) and id != "" and is_map(event) do
    group = RaftManager.group_for_session(id)

    ReplicaRouter.call_on_replica(
      group,
      __MODULE__,
      :append_event_local,
      [id, event, opts],
      __MODULE__,
      :append_event_local,
      [id, event, opts],
      prefer_leader: true
    )
  end

  def append_event(_id, _event, _opts), do: {:error, :invalid_event}

  @doc false
  def append_event_local(id, event, opts \\ [])
      when is_binary(id) and id != "" and is_map(event) do
    with {:ok, server_id, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.process_command(
             {server_id, Node.self()},
             {:append_event, id, event, opts},
             @timeout
           ) do
        {:ok, {:reply, {:ok, reply}}, _leader} -> {:ok, reply}
        {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
        {:timeout, leader} -> {:timeout, leader}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def ack_archived(id, upto_seq) when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    group = RaftManager.group_for_session(id)

    ReplicaRouter.call_on_replica(
      group,
      __MODULE__,
      :ack_archived_local,
      [id, upto_seq],
      __MODULE__,
      :ack_archived_local,
      [id, upto_seq],
      prefer_leader: true
    )
  end

  @doc false
  def ack_archived_local(id, upto_seq)
      when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    with {:ok, server_id, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.process_command(
             {server_id, Node.self()},
             {:ack_archived, id, upto_seq},
             @timeout
           ) do
        {:ok, {:reply, {:ok, reply}}, _leader} -> {:ok, reply}
        {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
        {:timeout, leader} -> {:timeout, leader}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp locate(id) do
    group = RaftManager.group_for_session(id)
    server_id = RaftManager.server_id(group)
    {:ok, server_id, group}
  end

  defp ensure_group_started(group_id) do
    case RaftManager.start_group(group_id) do
      :ok -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, {:shutdown, {:failed_to_start_child, _child, {:already_started, _pid}}}} -> :ok
      {:error, reason} -> {:error, reason}
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
