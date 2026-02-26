defmodule Starcite.DataPlane.RaftManager do
  @moduledoc """
  Utility module for static Raft write-group placement and lifecycle.

  Responsibilities:

  - Map sessions to write groups.
  - Compute static replica sets from configured write nodes.
  - Bootstrap or join Raft groups hosted by this node.
  """

  require Logger

  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.DataPlane.RaftFSM
  @server_ids_cache_key {__MODULE__, :server_ids}
  @cluster_names_cache_key {__MODULE__, :cluster_names}
  @ra_runtime_start_wait_attempts 20
  @ra_runtime_start_wait_sleep_ms 50
  @group_election_timeout_ms 500

  @doc false
  @spec validate_config!() :: :ok
  def validate_config! do
    WriteNodes.validate!()
  end

  @doc false
  @spec num_groups() :: pos_integer()
  def num_groups do
    WriteNodes.num_groups()
  end

  @doc false
  @spec replication_factor() :: pos_integer()
  def replication_factor do
    WriteNodes.replication_factor()
  end

  @doc false
  @spec write_nodes() :: [node()]
  def write_nodes do
    WriteNodes.nodes()
  end

  @doc false
  @spec write_node?(node()) :: boolean()
  def write_node?(node) when is_atom(node) do
    WriteNodes.write_node?(node)
  end

  @doc false
  @spec raft_data_dir_root() :: String.t()
  def raft_data_dir_root do
    Application.fetch_env!(:starcite, :raft_data_dir)
  end

  @doc false
  @spec ra_system_data_dir() :: String.t()
  def ra_system_data_dir do
    Path.join(raft_data_dir_root(), "ra_system")
  end

  @doc "Map session_id -> group_id (0..num_groups-1)"
  @spec group_for_session(binary()) :: non_neg_integer()
  def group_for_session(session_id) when is_binary(session_id) do
    :erlang.phash2(session_id, num_groups())
  end

  @doc "Get Ra server ID (process name) for a group"
  @spec server_id(non_neg_integer()) :: atom()
  def server_id(group_id) when is_integer(group_id) and group_id >= 0 do
    if group_id < num_groups() do
      server_ids() |> elem(group_id)
    else
      raise ArgumentError, "invalid group_id: #{inspect(group_id)}"
    end
  end

  @doc "Get cluster name for Ra API calls"
  @spec cluster_name(non_neg_integer()) :: atom()
  def cluster_name(group_id) when is_integer(group_id) and group_id >= 0 do
    if group_id < num_groups() do
      cluster_names() |> elem(group_id)
    else
      raise ArgumentError, "invalid group_id: #{inspect(group_id)}"
    end
  end

  @doc "Stable Ra member UID for a specific group/node pair."
  @spec member_uid(non_neg_integer(), node()) :: String.t()
  def member_uid(group_id, node \\ Node.self())
      when is_integer(group_id) and group_id >= 0 and is_atom(node) do
    "raft_group_#{group_id}_#{safe_node_name(node)}"
  end

  @doc "Determine the static write replicas for a group."
  @spec replicas_for_group(non_neg_integer()) :: [node()]
  def replicas_for_group(group_id) when is_integer(group_id) and group_id >= 0 do
    nodes = write_nodes()
    factor = replication_factor()

    nodes
    |> Enum.map(fn node ->
      score = :erlang.phash2({group_id, node})
      {score, node}
    end)
    |> Enum.sort()
    |> Enum.take(factor)
    |> Enum.map(fn {_score, node} -> node end)
  end

  @doc "Check if this node should host the given group."
  @spec should_participate?(non_neg_integer()) :: boolean()
  def should_participate?(group_id) when is_integer(group_id) and group_id >= 0 do
    Node.self() in replicas_for_group(group_id)
  end

  @doc """
  Start a Raft group on this node when this node is in the group's write replica set.

  Returns `:ok` for non-participating nodes so callers can be idempotent.
  """
  @spec start_group(non_neg_integer()) :: :ok | {:error, term()}
  def start_group(group_id) when is_integer(group_id) and group_id >= 0 do
    if should_participate?(group_id) do
      do_start_group(group_id)
    else
      :ok
    end
  end

  defp do_start_group(group_id) do
    local_server_name = server_id(group_id)

    if Process.whereis(local_server_name) do
      :ok
    else
      with :ok <- ensure_runtime_started() do
        case restart_or_start_local_server(group_id) do
          :ok ->
            _ = maybe_trigger_local_election(group_id)
            :ok

          {:error, {:already_started, _pid}} ->
            :ok

          {:error, {:shutdown, {:failed_to_start_child, _child, {:already_started, _pid}}}} ->
            :ok

          {:error, reason} ->
            if runtime_boot_error?(reason) do
              Logger.warning(
                "RaftManager: Group #{group_id} unavailable due to RA runtime state: #{inspect(reason)}"
              )

              _ = ensure_runtime_started()
              {:error, :ra_runtime_unavailable}
            else
              Logger.error("RaftManager: Failed to start group #{group_id}: #{inspect(reason)}")
              {:error, reason}
            end
        end
      end
    end
  end

  defp restart_or_start_local_server(group_id) do
    local_server_id = {server_id(group_id), Node.self()}

    case :ra.restart_server(:default, local_server_id) do
      :ok ->
        Logger.debug("RaftManager: Restarted group #{group_id} from persisted state")
        :ok

      {:error, reason} when reason in [:name_not_registered, :not_found] ->
        start_local_server(group_id)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp start_local_server(group_id) do
    server_name = server_id(group_id)
    cluster_name = cluster_name(group_id)
    machine = {:module, RaftFSM, %{group_id: group_id}}
    my_node = Node.self()
    node_name = safe_node_name(my_node)

    initial_members =
      for node <- replicas_for_group(group_id), is_atom(node), do: {server_name, node}

    data_dir_root = raft_data_dir_root()
    data_dir = Path.join([data_dir_root, "group_#{group_id}", node_name])

    case File.mkdir_p(data_dir) do
      :ok ->
        system_config = :ra_system.default_config() |> Map.put(:data_dir, data_dir)

        server_conf = %{
          id: {server_name, my_node},
          uid: member_uid(group_id, my_node),
          cluster_name: cluster_name,
          initial_members: initial_members,
          log_init_args: %{
            uid: "raft_log_#{group_id}_#{node_name}",
            system_config: system_config
          },
          machine: machine
        }

        case :ra.start_server(:default, server_conf) do
          :ok ->
            Logger.debug("RaftManager: Started fresh server for group #{group_id}")
            :ok

          {:error, :not_new} ->
            :ra.restart_server(:default, {server_name, my_node})

          {:error, {:already_started, _}} ->
            :ok

          {:error, {:shutdown, {:failed_to_start_child, _, {:already_started, _}}}} ->
            Logger.debug("RaftManager: Group #{group_id} already running locally")
            :ok

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error(
          "RaftManager: Failed to create raft data dir for group #{group_id}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp maybe_trigger_local_election(group_id) when is_integer(group_id) and group_id >= 0 do
    server_ref = {server_id(group_id), Node.self()}

    try do
      :ok = :ra.trigger_election(server_ref, @group_election_timeout_ms)
      :ok
    catch
      :exit, _reason ->
        :ok
    end
  end

  @doc false
  @spec machine(non_neg_integer()) :: {:module, module(), map()}
  def machine(group_id) when is_integer(group_id) and group_id >= 0 do
    {:module, RaftFSM, %{group_id: group_id}}
  end

  @doc false
  @spec ensure_runtime_started() :: :ok | {:error, term()}
  def ensure_runtime_started do
    if ra_runtime_ready?() do
      :ok
    else
      with :ok <- start_ra_runtime(),
           :ok <- wait_for_ra_runtime(@ra_runtime_start_wait_attempts) do
        :ok
      end
    end
  end

  defp safe_node_name(node) when is_atom(node) do
    node |> Atom.to_string() |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
  end

  defp start_ra_runtime do
    case :application.ensure_all_started(:ra) do
      {:ok, _started_apps} ->
        :ok

      {:error, reason} = error ->
        Logger.warning("RaftManager: failed to start :ra runtime: #{inspect(reason)}")
        error
    end
  end

  defp wait_for_ra_runtime(0), do: {:error, :ra_runtime_unavailable}

  defp wait_for_ra_runtime(attempts) when is_integer(attempts) and attempts > 0 do
    if ra_runtime_ready?() do
      :ok
    else
      Process.sleep(@ra_runtime_start_wait_sleep_ms)
      wait_for_ra_runtime(attempts - 1)
    end
  end

  defp ra_runtime_ready? do
    Process.whereis(:ra_server_sup_sup) != nil and :ets.whereis(:ra_directory) != :undefined
  end

  defp runtime_boot_error?(reason) do
    reason_text = inspect(reason)

    String.contains?(reason_text, "ra_directory") or
      String.contains?(reason_text, "ra_server_sup_sup")
  end

  defp server_ids do
    build_cached_group_atoms(@server_ids_cache_key, "raft_group_")
  end

  defp cluster_names do
    build_cached_group_atoms(@cluster_names_cache_key, "raft_cluster_")
  end

  defp build_cached_group_atoms(cache_key, prefix)
       when is_tuple(cache_key) and is_binary(prefix) do
    total_groups = num_groups()

    case :persistent_term.get(cache_key, :undefined) do
      {^total_groups, values} when is_tuple(values) ->
        values

      _ ->
        values =
          0..(total_groups - 1)
          |> Enum.map(&String.to_atom("#{prefix}#{&1}"))
          |> List.to_tuple()

        :persistent_term.put(cache_key, {total_groups, values})
        values
    end
  end
end
