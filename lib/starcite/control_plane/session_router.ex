defmodule Starcite.ControlPlane.SessionRouter do
  @moduledoc """
  Control-plane routing helper for session-scoped operations.

  Hot-path modules should call this boundary instead of direct Raft/router APIs.
  """

  alias Starcite.ControlPlane.ReplicaRouter
  alias Starcite.DataPlane.RaftManager

  @owner_probe_timeout_ms Application.compile_env(:starcite, :session_owner_probe_timeout_ms, 50)
  @owner_epoch_probe_timeout_ms Application.compile_env(
                                  :starcite,
                                  :session_owner_epoch_probe_timeout_ms,
                                  50
                                )

  @spec call(
          String.t(),
          module(),
          atom(),
          [term()],
          module(),
          atom(),
          [term()],
          keyword()
        ) :: term()
  def call(
        session_id,
        remote_module,
        remote_fun,
        remote_args,
        local_module,
        local_fun,
        local_args,
        route_opts \\ []
      )
      when is_binary(session_id) and session_id != "" and is_atom(remote_module) and
             is_atom(remote_fun) and is_list(remote_args) and is_atom(local_module) and
             is_atom(local_fun) and is_list(local_args) and is_list(route_opts) do
    group_id = RaftManager.group_for_session(session_id)

    ReplicaRouter.call_on_replica(
      group_id,
      remote_module,
      remote_fun,
      remote_args,
      local_module,
      local_fun,
      local_args,
      route_opts
    )
  end

  @spec ensure_local_owner(String.t(), keyword()) ::
          :ok | {:error, :not_leader | {:not_leader, {atom(), node()}}}
  def ensure_local_owner(session_id, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_list(opts) do
    group_id = RaftManager.group_for_session(session_id)
    server_id = RaftManager.server_id(group_id)
    self_node = Keyword.get(opts, :self, Node.self())

    if singleton_replica_group_owner?(group_id, self_node) do
      :ok
    else
      leader_hint = resolve_leader_hint(group_id, server_id, self_node, opts)

      case leader_hint do
        {^server_id, leader_node}
        when is_atom(leader_node) and not is_nil(leader_node) and leader_node == self_node ->
          :ok

        {^server_id, leader_node}
        when is_atom(leader_node) and not is_nil(leader_node) ->
          {:error, {:not_leader, {server_id, leader_node}}}

        _other ->
          {:error, :not_leader}
      end
    end
  end

  @spec local_owner_epoch(String.t(), non_neg_integer(), keyword()) :: non_neg_integer()
  def local_owner_epoch(session_id, fallback_epoch \\ 0, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_integer(fallback_epoch) and
             fallback_epoch >= 0 and is_list(opts) do
    group_id = RaftManager.group_for_session(session_id)
    server_id = RaftManager.server_id(group_id)
    self_node = Keyword.get(opts, :self, Node.self())

    case Keyword.fetch(opts, :metrics) do
      {:ok, metrics} ->
        term_from_metrics(metrics, fallback_epoch)

      :error ->
        timeout = owner_epoch_probe_timeout_ms(opts)

        case :ra.key_metrics({server_id, self_node}, timeout) do
          metrics when is_map(metrics) -> term_from_metrics(metrics, fallback_epoch)
          _other -> fallback_epoch
        end
    end
  end

  @spec replica_nodes(String.t()) :: [node()]
  def replica_nodes(session_id) when is_binary(session_id) and session_id != "" do
    session_id
    |> RaftManager.group_for_session()
    |> RaftManager.replicas_for_group()
  end

  defp resolve_leader_hint(group_id, server_id, self_node, opts)
       when is_integer(group_id) and group_id >= 0 and is_atom(server_id) and
              is_atom(self_node) and is_list(opts) do
    case Keyword.fetch(opts, :leader_hint) do
      {:ok, leader_hint} ->
        leader_hint

      :error ->
        lookup_or_probe_leader(group_id, server_id, self_node, opts)
    end
  end

  defp lookup_or_probe_leader(group_id, server_id, self_node, opts)
       when is_integer(group_id) and group_id >= 0 and is_atom(server_id) and
              is_atom(self_node) and is_list(opts) do
    cluster_name = RaftManager.cluster_name(group_id)

    case :ra_leaderboard.lookup_leader(cluster_name) do
      {^server_id, leader_node} = leader_hint
      when is_atom(leader_node) and not is_nil(leader_node) ->
        leader_hint

      _other ->
        probe_leader(server_id, self_node, opts)
    end
  end

  defp probe_leader(server_id, self_node, opts)
       when is_atom(server_id) and is_atom(self_node) and is_list(opts) do
    probe_timeout_ms =
      case Keyword.get(opts, :probe_timeout_ms, @owner_probe_timeout_ms) do
        timeout when is_integer(timeout) and timeout > 0 -> timeout
        _other -> @owner_probe_timeout_ms
      end

    case :ra.members({server_id, self_node}, probe_timeout_ms) do
      {:ok, _members, {^server_id, leader_node}}
      when is_atom(leader_node) and not is_nil(leader_node) ->
        {server_id, leader_node}

      _other ->
        nil
    end
  end

  defp owner_epoch_probe_timeout_ms(opts) when is_list(opts) do
    case Keyword.get(opts, :epoch_probe_timeout_ms, @owner_epoch_probe_timeout_ms) do
      timeout when is_integer(timeout) and timeout > 0 -> timeout
      _other -> @owner_epoch_probe_timeout_ms
    end
  end

  defp term_from_metrics(metrics, fallback_epoch)
       when is_map(metrics) and is_integer(fallback_epoch) and fallback_epoch >= 0 do
    case Map.get(metrics, :term) do
      term when is_integer(term) and term >= 0 ->
        max(term, fallback_epoch)

      _other ->
        fallback_epoch
    end
  end

  defp singleton_replica_group_owner?(group_id, self_node)
       when is_integer(group_id) and group_id >= 0 and is_atom(self_node) do
    case RaftManager.replicas_for_group(group_id) do
      [^self_node] -> true
      _other -> false
    end
  end
end
