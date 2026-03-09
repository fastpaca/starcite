defmodule Starcite.Routing.SessionRouter do
  @moduledoc """
  Khepri-backed routing helper for session-scoped operations.

  The routing contract consumed by the data plane stays small:

  - route a call to the current owner
  - check whether the local node is the owner
  - fetch the current fencing epoch
  - fetch replica membership for in-memory data-plane replication
  """

  alias Starcite.Routing.{Store, Watcher}

  @rpc_timeout_ms Application.compile_env(:starcite, :session_router_rpc_timeout_ms, 5_000)

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
    with {:ok, assignment} <- assignment_for_call(session_id, route_opts) do
      dispatch_to_owner(
        session_id,
        assignment.owner,
        remote_module,
        remote_fun,
        remote_args,
        local_module,
        local_fun,
        local_args,
        route_opts,
        1
      )
    end
  end

  @spec ensure_local_owner(String.t(), keyword()) ::
          :ok | {:error, :not_leader | {:not_leader, {atom(), node()}}}
  def ensure_local_owner(session_id, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_list(opts) do
    self_node = Keyword.get(opts, :self, Node.self())

    case assignment_for_owner_check(session_id, opts) do
      {:ok, %{owner: ^self_node} = assignment}
      when not is_map_key(assignment, :status) or assignment.status == :active ->
        :ok

      {:ok, %{status: :moving}} ->
        {:error, :ownership_transfer_in_progress}

      {:ok, %{owner: owner}} when is_atom(owner) ->
        {:error, {:not_leader, {:session_owner, owner}}}

      {:error, :not_found} ->
        {:error, :not_leader}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec local_owner_epoch(String.t(), non_neg_integer(), keyword()) :: non_neg_integer()
  def local_owner_epoch(session_id, fallback_epoch \\ 0, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_integer(fallback_epoch) and
             fallback_epoch >= 0 and is_list(opts) do
    case assignment_for_epoch(session_id, opts) do
      {:ok, %{epoch: epoch}} when is_integer(epoch) and epoch >= 0 ->
        max(epoch, fallback_epoch)

      _other ->
        fallback_epoch
    end
  end

  @spec replica_nodes(String.t()) :: [node()]
  def replica_nodes(session_id) when is_binary(session_id) and session_id != "" do
    case Store.ensure_assignment(session_id) do
      {:ok, %{replicas: replicas}} when is_list(replicas) and replicas != [] ->
        replicas

      _other ->
        [Node.self()]
    end
  end

  defp assignment_for_owner_check(session_id, opts)
       when is_binary(session_id) and session_id != "" and is_list(opts) do
    case Keyword.fetch(opts, :assignment) do
      {:ok, assignment} when is_map(assignment) ->
        {:ok, assignment}

      _other ->
        Store.get_assignment(session_id, favor: :consistency)
    end
  end

  defp assignment_for_epoch(session_id, opts)
       when is_binary(session_id) and session_id != "" and is_list(opts) do
    case Keyword.fetch(opts, :assignment) do
      {:ok, assignment} when is_map(assignment) ->
        {:ok, assignment}

      _other ->
        Store.get_assignment(session_id, favor: :consistency)
    end
  end

  defp assignment_for_call(session_id, route_opts)
       when is_binary(session_id) and session_id != "" and is_list(route_opts) do
    case Keyword.fetch(route_opts, :assignment) do
      {:ok, assignment} when is_map(assignment) ->
        {:ok, assignment}

      _other ->
        Store.ensure_assignment(session_id)
    end
  end

  defp dispatch_to_owner(
         session_id,
         owner,
         remote_module,
         remote_fun,
         remote_args,
         local_module,
         local_fun,
         local_args,
         route_opts,
         refreshes_remaining
       )
       when is_binary(session_id) and session_id != "" and is_atom(owner) and
              is_atom(remote_module) and
              is_atom(remote_fun) and is_list(remote_args) and is_atom(local_module) and
              is_atom(local_fun) and is_list(local_args) and is_list(route_opts) and
              is_integer(refreshes_remaining) and refreshes_remaining >= 0 do
    self_node = Keyword.get(route_opts, :self, Node.self())

    if owner == self_node do
      apply(local_module, local_fun, local_args)
    else
      if Watcher.suspect?(owner) do
        {:error, {:routing_rpc_failed, owner, :suspect}}
      else
        case rpc_call(owner, remote_module, remote_fun, remote_args, route_opts) do
          {:error, {:not_leader, {:session_owner, redirect_owner}}}
          when is_atom(redirect_owner) and redirect_owner != owner and refreshes_remaining > 0 ->
            reroute_to_assignment(
              session_id,
              owner,
              remote_module,
              remote_fun,
              remote_args,
              local_module,
              local_fun,
              local_args,
              route_opts,
              refreshes_remaining - 1
            )

          {:error, {:not_leader, {:session_owner, _redirect_owner}}} ->
            {:error, {:routing_rpc_failed, owner, :stale_assignment}}

          {:badrpc, reason} ->
            {:error, {:routing_rpc_failed, owner, reason}}

          other ->
            other
        end
      end
    end
  end

  defp reroute_to_assignment(
         session_id,
         previous_owner,
         remote_module,
         remote_fun,
         remote_args,
         local_module,
         local_fun,
         local_args,
         route_opts,
         refreshes_remaining
       )
       when is_binary(session_id) and session_id != "" and is_atom(previous_owner) and
              is_atom(remote_module) and is_atom(remote_fun) and is_list(remote_args) and
              is_atom(local_module) and is_atom(local_fun) and is_list(local_args) and
              is_list(route_opts) and is_integer(refreshes_remaining) and
              refreshes_remaining >= 0 do
    case refresh_assignment(session_id, route_opts) do
      {:ok, %{status: :moving}} ->
        {:error, :ownership_transfer_in_progress}

      {:ok, %{owner: owner}} when is_atom(owner) and owner != previous_owner ->
        dispatch_to_owner(
          session_id,
          owner,
          remote_module,
          remote_fun,
          remote_args,
          local_module,
          local_fun,
          local_args,
          route_opts,
          refreshes_remaining
        )

      {:ok, %{owner: ^previous_owner}} ->
        {:error, {:routing_rpc_failed, previous_owner, :stale_assignment}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp refresh_assignment(session_id, route_opts)
       when is_binary(session_id) and session_id != "" and is_list(route_opts) do
    case Keyword.fetch(route_opts, :assignment_fetcher) do
      {:ok, assignment_fetcher} when is_function(assignment_fetcher, 1) ->
        assignment_fetcher.(session_id)

      _other ->
        Store.get_assignment(session_id, favor: :consistency)
    end
  end

  defp rpc_call(owner, remote_module, remote_fun, remote_args, route_opts)
       when is_atom(owner) and is_atom(remote_module) and is_atom(remote_fun) and
              is_list(remote_args) and is_list(route_opts) do
    case Keyword.fetch(route_opts, :rpc_fun) do
      {:ok, rpc_fun} when is_function(rpc_fun, 4) ->
        rpc_fun.(owner, remote_module, remote_fun, remote_args)

      _other ->
        :rpc.call(owner, remote_module, remote_fun, remote_args, @rpc_timeout_ms)
    end
  end
end
