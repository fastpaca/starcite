defmodule Starcite.Routing.SessionRouter do
  @moduledoc """
  Khepri-backed routing helper for session-scoped operations.

  The routing contract consumed by the data plane stays small:

  - route a call to the current owner
  - check whether the local node is the owner
  - fetch the current fencing epoch
  - fetch replica membership for in-memory data-plane replication
  """

  alias Starcite.Observability.Telemetry
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
    self_node = Keyword.get(route_opts, :self, Node.self())
    route_started_at = System.monotonic_time()

    case assignment_for_call(session_id, route_opts) do
      {:ok, assignment} ->
        emit_request_route_phase(route_opts, {:ok, :owner_selected}, route_started_at)
        dispatch_started_at = System.monotonic_time()

        {result, refreshes, target_node} =
          dispatch_call(
            session_id,
            assignment,
            remote_module,
            remote_fun,
            remote_args,
            local_module,
            local_fun,
            local_args,
            route_opts
          )

        emit_request_dispatch_phase(route_opts, result, dispatch_started_at)

        :ok =
          Telemetry.routing_result(
            routing_target(target_node, self_node),
            routing_telemetry_result(result),
            refreshes
          )

        result

      {:error, _reason} = error ->
        emit_request_route_phase(route_opts, error, route_started_at)
        :ok = Telemetry.routing_result(:unassigned, error, 0)
        error
    end
  end

  @spec ensure_local_owner(String.t(), keyword()) ::
          :ok | {:error, :not_leader | {:not_leader, {atom(), node()}}}
  def ensure_local_owner(session_id, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_list(opts) do
    self_node = Keyword.get(opts, :self, Node.self())

    case consistent_assignment(session_id, opts) do
      {:ok, %{owner: ^self_node} = assignment}
      when not is_map_key(assignment, :status) or assignment.status == :active ->
        :ok

      {:ok, %{status: :moving}} ->
        :ok =
          Telemetry.routing_fence(session_id, :session_router, :ownership_transfer_in_progress)

        {:error, :ownership_transfer_in_progress}

      {:ok, %{owner: owner}} when is_atom(owner) ->
        :ok = Telemetry.routing_fence(session_id, :session_router, :not_leader)
        {:error, {:not_leader, {:session_owner, owner}}}

      {:error, :not_found} ->
        :ok = Telemetry.routing_fence(session_id, :session_router, :not_leader)
        {:error, :not_leader}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec local_owner_epoch(String.t(), non_neg_integer(), keyword()) :: non_neg_integer()
  def local_owner_epoch(session_id, fallback_epoch \\ 0, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_integer(fallback_epoch) and
             fallback_epoch >= 0 and is_list(opts) do
    case consistent_assignment(session_id, opts) do
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

  defp assignment_for_call(session_id, route_opts)
       when is_binary(session_id) and session_id != "" and is_list(route_opts) do
    assignment_or(route_opts, fn -> Store.ensure_assignment(session_id) end)
  end

  defp consistent_assignment(session_id, opts)
       when is_binary(session_id) and session_id != "" and is_list(opts) do
    assignment_or(opts, fn -> Store.get_assignment(session_id, favor: :consistency) end)
  end

  defp assignment_or(opts, fallback) when is_list(opts) and is_function(fallback, 0) do
    case Keyword.fetch(opts, :assignment) do
      {:ok, assignment} when is_map(assignment) -> {:ok, assignment}
      _other -> fallback.()
    end
  end

  defp dispatch_call(
         session_id,
         %{owner: owner} = assignment,
         remote_module,
         remote_fun,
         remote_args,
         local_module,
         local_fun,
         local_args,
         route_opts
       )
       when is_binary(session_id) and session_id != "" and is_atom(owner) and is_map(assignment) and
              is_atom(remote_module) and
              is_atom(remote_fun) and is_list(remote_args) and is_atom(local_module) and
              is_atom(local_fun) and is_list(local_args) and is_list(route_opts) do
    if Keyword.get(route_opts, :prefer_leader, true) do
      {result, refreshes} =
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
          1,
          0
        )

      {result, refreshes, owner}
    else
      dispatch_to_replicas(
        session_id,
        assignment,
        remote_module,
        remote_fun,
        remote_args,
        local_module,
        local_fun,
        local_args,
        route_opts,
        1,
        0
      )
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
         refreshes_remaining,
         refresh_count
       )
       when is_binary(session_id) and session_id != "" and is_atom(owner) and
              is_atom(remote_module) and
              is_atom(remote_fun) and is_list(remote_args) and is_atom(local_module) and
              is_atom(local_fun) and is_list(local_args) and is_list(route_opts) and
              is_integer(refreshes_remaining) and refreshes_remaining >= 0 and
              is_integer(refresh_count) and refresh_count >= 0 do
    self_node = Keyword.get(route_opts, :self, Node.self())

    cond do
      owner == self_node ->
        {apply(local_module, local_fun, local_args), refresh_count}

      Watcher.suspect?(owner) ->
        {{:error, {:routing_rpc_failed, owner, :suspect}}, refresh_count}

      true ->
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
              refreshes_remaining - 1,
              refresh_count + 1
            )

          {:error, {:not_leader, {:session_owner, _redirect_owner}}} ->
            {{:error, {:routing_rpc_failed, owner, :stale_assignment}}, refresh_count}

          {:badrpc, reason} ->
            {{:error, {:routing_rpc_failed, owner, reason}}, refresh_count}

          other ->
            {other, refresh_count}
        end
    end
  end

  defp dispatch_to_replicas(
         session_id,
         assignment,
         remote_module,
         remote_fun,
         remote_args,
         local_module,
         local_fun,
         local_args,
         route_opts,
         refreshes_remaining,
         refresh_count
       )
       when is_binary(session_id) and session_id != "" and is_map(assignment) and
              is_atom(remote_module) and
              is_atom(remote_fun) and is_list(remote_args) and is_atom(local_module) and
              is_atom(local_fun) and is_list(local_args) and is_list(route_opts) and
              is_integer(refreshes_remaining) and refreshes_remaining >= 0 and
              is_integer(refresh_count) and refresh_count >= 0 do
    case dispatch_to_replica_candidates(
           replica_read_candidates(session_id, assignment, route_opts),
           remote_module,
           remote_fun,
           remote_args,
           local_module,
           local_fun,
           local_args,
           route_opts,
           []
         ) do
      {{:error, {:no_available_replicas, _failures}} = error, nil}
      when refreshes_remaining > 0 ->
        case refresh_assignment(session_id, route_opts) do
          {:ok, refreshed_assignment} when is_map(refreshed_assignment) ->
            dispatch_to_replicas(
              session_id,
              refreshed_assignment,
              remote_module,
              remote_fun,
              remote_args,
              local_module,
              local_fun,
              local_args,
              route_opts,
              refreshes_remaining - 1,
              refresh_count + 1
            )

          {:error, _reason} ->
            {error, refresh_count, nil}
        end

      {result, target_node} ->
        {result, refresh_count, target_node}
    end
  end

  defp dispatch_to_replica_candidates(
         [],
         _remote_module,
         _remote_fun,
         _remote_args,
         _local_module,
         _local_fun,
         _local_args,
         _route_opts,
         failures
       ) do
    {{:error, {:no_available_replicas, Enum.reverse(failures)}}, nil}
  end

  defp dispatch_to_replica_candidates(
         [candidate | rest],
         remote_module,
         remote_fun,
         remote_args,
         local_module,
         local_fun,
         local_args,
         route_opts,
         failures
       )
       when is_atom(candidate) and is_atom(remote_module) and is_atom(remote_fun) and
              is_list(remote_args) and is_atom(local_module) and is_atom(local_fun) and
              is_list(local_args) and is_list(route_opts) and is_list(failures) do
    case dispatch_to_candidate(
           candidate,
           remote_module,
           remote_fun,
           remote_args,
           local_module,
           local_fun,
           local_args,
           route_opts
         ) do
      {:retry, failure} ->
        dispatch_to_replica_candidates(
          rest,
          remote_module,
          remote_fun,
          remote_args,
          local_module,
          local_fun,
          local_args,
          route_opts,
          [failure | failures]
        )

      {:ok, result} ->
        {result, candidate}
    end
  end

  defp dispatch_to_candidate(
         candidate,
         remote_module,
         remote_fun,
         remote_args,
         local_module,
         local_fun,
         local_args,
         route_opts
       )
       when is_atom(candidate) and is_atom(remote_module) and is_atom(remote_fun) and
              is_list(remote_args) and is_atom(local_module) and is_atom(local_fun) and
              is_list(local_args) and is_list(route_opts) do
    self_node = Keyword.get(route_opts, :self, Node.self())

    result =
      cond do
        candidate == self_node ->
          apply(local_module, local_fun, local_args)

        Watcher.suspect?(candidate) ->
          {:error, {:routing_rpc_failed, candidate, :suspect}}

        true ->
          case rpc_call(candidate, remote_module, remote_fun, remote_args, route_opts) do
            {:badrpc, reason} -> {:error, {:routing_rpc_failed, candidate, reason}}
            other -> other
          end
      end

    if retryable_replica_result?(result) do
      {:retry, {candidate, result}}
    else
      {:ok, result}
    end
  end

  defp replica_read_candidates(session_id, assignment, route_opts)
       when is_binary(session_id) and session_id != "" and is_map(assignment) and
              is_list(route_opts) do
    self_node = Keyword.get(route_opts, :self, Node.self())
    owner = Map.get(assignment, :owner)

    replicas =
      case Map.get(assignment, :replicas) do
        replicas when is_list(replicas) and replicas != [] -> replicas
        _other when is_atom(owner) -> [owner]
        _other -> []
      end

    replicas = Enum.uniq(Enum.filter(replicas, &is_atom/1))

    local =
      if self_node in replicas do
        [self_node]
      else
        []
      end

    remote_followers =
      replicas
      |> Enum.reject(&(&1 in [self_node, owner]))
      |> rotate_replica_candidates(session_id, self_node)

    remote_owner =
      if is_atom(owner) and owner != self_node and owner in replicas do
        [owner]
      else
        []
      end

    local ++ remote_followers ++ remote_owner
  end

  defp rotate_replica_candidates([], _session_id, _self_node), do: []

  defp rotate_replica_candidates(candidates, session_id, self_node)
       when is_list(candidates) and is_binary(session_id) and session_id != "" and
              is_atom(self_node) do
    offset = rem(:erlang.phash2({session_id, self_node}), length(candidates))
    {prefix, suffix} = Enum.split(candidates, offset)
    suffix ++ prefix
  end

  defp retryable_replica_result?({:timeout, _reason}), do: true
  defp retryable_replica_result?({:error, {:timeout, _reason}}), do: true
  defp retryable_replica_result?({:error, {:routing_rpc_failed, _node, _reason}}), do: true
  defp retryable_replica_result?({:error, :session_log_unavailable}), do: true
  defp retryable_replica_result?({:error, {:session_owner, _node}}), do: true
  defp retryable_replica_result?({:error, {:not_leader, {:session_owner, _node}}}), do: true
  defp retryable_replica_result?({:error, :not_leader}), do: true
  defp retryable_replica_result?(_result), do: false

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
         refreshes_remaining,
         refresh_count
       )
       when is_binary(session_id) and session_id != "" and is_atom(previous_owner) and
              is_atom(remote_module) and is_atom(remote_fun) and is_list(remote_args) and
              is_atom(local_module) and is_atom(local_fun) and is_list(local_args) and
              is_list(route_opts) and is_integer(refreshes_remaining) and
              refreshes_remaining >= 0 and is_integer(refresh_count) and refresh_count >= 0 do
    case refresh_assignment(session_id, route_opts) do
      {:ok, %{status: :moving}} ->
        :ok =
          Telemetry.routing_fence(session_id, :session_router, :ownership_transfer_in_progress)

        {{:error, :ownership_transfer_in_progress}, refresh_count}

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
          refreshes_remaining,
          refresh_count
        )

      {:ok, %{owner: ^previous_owner}} ->
        :ok = Telemetry.routing_fence(session_id, :session_router, :not_leader)
        {{:error, {:routing_rpc_failed, previous_owner, :stale_assignment}}, refresh_count}

      {:error, reason} ->
        {{:error, reason}, refresh_count}
    end
  end

  defp routing_target(target_node, self_node)
       when is_atom(target_node) and is_atom(self_node) do
    if target_node == self_node, do: :local, else: :remote
  end

  defp routing_target(_target_node, _self_node), do: :unassigned

  defp routing_telemetry_result({:timeout, _reason} = timeout), do: timeout
  defp routing_telemetry_result({:error, {:timeout, _reason}} = error), do: error

  defp routing_telemetry_result({:error, {:routing_rpc_failed, _node, _reason}} = error),
    do: error

  defp routing_telemetry_result({:error, {:no_available_replicas, _failures}} = error), do: error
  defp routing_telemetry_result({:error, :ownership_transfer_in_progress} = error), do: error
  defp routing_telemetry_result({:error, :no_ready_cluster_nodes} = error), do: error
  defp routing_telemetry_result(_result), do: {:ok, :delivered}

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

  defp emit_request_route_phase(route_opts, result, started_at)
       when is_list(route_opts) and is_integer(started_at) do
    case Keyword.get(route_opts, :request_operation) do
      operation when operation in [:append_event, :append_events] ->
        duration_ms = elapsed_ms_since(started_at)
        :ok = Telemetry.request_result(operation, :route, result, duration_ms)

      _other ->
        :ok
    end
  end

  defp emit_request_dispatch_phase(route_opts, result, started_at)
       when is_list(route_opts) and is_integer(started_at) do
    case Keyword.get(route_opts, :request_operation) do
      operation when operation in [:append_event, :append_events] ->
        duration_ms = elapsed_ms_since(started_at)
        :ok = Telemetry.request_result(operation, :dispatch, result, duration_ms)

      _other ->
        :ok
    end
  end

  defp elapsed_ms_since(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end
end
