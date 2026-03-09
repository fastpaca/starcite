defmodule Starcite.Routing.Store do
  @moduledoc """
  Khepri-backed routing ownership store.

  The routing store is the durable control-plane record for active sessions:

  - `session_id -> %{owner, epoch, replicas, status}`
  - node lifecycle and lease state under `[:nodes, node_name]`

  It is intentionally small. The data plane owns sequencing and
  replication; this store only answers who owns a session, which epoch that
  ownership is fenced with, and which nodes are eligible to receive traffic.
  """

  use GenServer

  require Logger

  alias Starcite.Routing.{Policy, Topology}

  @dialyzer {:nowarn_function,
             [
               do_compare_and_swap_assignment_local: 3,
               do_commit_transfer_local: 3,
               do_failover_assignment_local: 4,
               mutate_node_record: 4,
               exact_match_pattern: 1
             ]}

  @join_timeout_ms 15_000
  @cas_retry_limit 8
  @default_store_id :starcite_routing
  @default_store_dir "priv/khepri"
  @cluster_reconcile_interval_ms Application.compile_env(
                                   :starcite,
                                   :routing_store_reconcile_interval_ms,
                                   1_000
                                 )

  @type assignment :: %{
          required(:owner) => node(),
          required(:epoch) => non_neg_integer(),
          required(:replicas) => [node()],
          required(:status) => :active | :moving,
          required(:updated_at_ms) => integer(),
          optional(:target_owner) => node(),
          optional(:transfer_id) => String.t()
        }
  @type node_lifecycle :: :ready | :draining | :drained
  @type node_record :: %{
          required(:status) => node_lifecycle(),
          required(:lease_until_ms) => integer(),
          required(:updated_at_ms) => integer()
        }

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @spec running?() :: boolean()
  def running?, do: Process.whereis(__MODULE__) != nil

  @spec store_id() :: atom()
  def store_id do
    Application.get_env(:starcite, :routing_store_id, @default_store_id)
  end

  @spec store_dir_root() :: String.t()
  def store_dir_root do
    Application.get_env(:starcite, :routing_store_dir, @default_store_dir)
  end

  @spec local_store_dir() :: String.t()
  def local_store_dir do
    Path.join(store_dir_root(), sanitize_node_name(Node.self()))
  end

  @spec clear() :: :ok | {:error, term()}
  def clear do
    route_command(:clear, [])
  end

  @spec get_assignment(String.t(), keyword()) :: {:ok, assignment()} | {:error, term()}
  def get_assignment(session_id, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_list(opts) do
    do_get_assignment(session_id, Keyword.get(opts, :favor, :low_latency))
  end

  @spec ensure_assignment(String.t()) :: {:ok, assignment()} | {:error, term()}
  def ensure_assignment(session_id) when is_binary(session_id) and session_id != "" do
    case do_get_assignment(session_id, :consistency) do
      {:ok, assignment} ->
        {:ok, assignment}

      {:error, :not_found} ->
        route_command(:claim_assignment, [session_id, now_ms()])

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec start_drain_transfers(node()) :: {:ok, non_neg_integer()} | {:error, term()}
  def start_drain_transfers(owner_node) when is_atom(owner_node) do
    started_at_ms = now_ms()

    with {:ok, assignments} <- all_assignments(:consistency),
         {:ok, node_records} <- all_node_records(:consistency) do
      ready_nodes = ready_node_list(node_records, started_at_ms)

      {started, _counts} =
        assignments
        |> Enum.filter(fn {_session_id, assignment} ->
          assignment.owner == owner_node and assignment.status == :active
        end)
        |> Enum.reduce({0, Policy.session_counts(assignments)}, fn {session_id, assignment},
                                                                   {acc, counts} ->
          case Policy.next_owner(assignment, owner_node, ready_nodes, counts) do
            nil ->
              {acc, counts}

            target_node ->
              transfer_id = new_transfer_id()

              next =
                assignment
                |> Map.put(:status, :moving)
                |> Map.put(:target_owner, target_node)
                |> Map.put(:transfer_id, transfer_id)
                |> Map.put(:updated_at_ms, started_at_ms)

              case compare_and_swap_assignment(session_id, assignment, next) do
                :ok -> {acc + 1, Map.update(counts, target_node, 1, &(&1 + 1))}
                {:error, _reason} -> {acc, counts}
              end
          end
        end)

      {:ok, started}
    end
  end

  @spec commit_transfer(String.t(), String.t()) :: {:ok, assignment()} | {:error, term()}
  def commit_transfer(session_id, transfer_id)
      when is_binary(session_id) and session_id != "" and is_binary(transfer_id) and
             transfer_id != "" do
    route_command(:commit_transfer, [session_id, transfer_id, now_ms()])
  end

  @spec drain_status(node()) ::
          {:ok, %{active_owned_sessions: non_neg_integer(), moving_sessions: non_neg_integer()}}
          | {:error, term()}
  def drain_status(node) when is_atom(node) do
    with {:ok, assignments} <- all_assignments(:consistency) do
      {active_owned_sessions, moving_sessions} =
        Enum.reduce(assignments, {0, 0}, fn
          {_session_id, %{owner: ^node, status: :active}}, {active_acc, moving_acc} ->
            {active_acc + 1, moving_acc}

          {_session_id, %{owner: ^node, status: :moving}}, {active_acc, moving_acc} ->
            {active_acc, moving_acc + 1}

          _other, acc ->
            acc
        end)

      {:ok,
       %{
         active_owned_sessions: active_owned_sessions,
         moving_sessions: moving_sessions
       }}
    end
  end

  @spec reassign_sessions_from(node()) :: {:ok, non_neg_integer()} | {:error, term()}
  def reassign_sessions_from(owner_node) when is_atom(owner_node) do
    failed_at_ms = now_ms()

    with {:ok, assignments} <- all_assignments(:consistency),
         {:ok, node_records} <- all_node_records(:consistency) do
      ready_nodes = ready_node_list(node_records, failed_at_ms)

      {moved, _counts} =
        assignments
        |> Enum.filter(fn {_session_id, assignment} -> assignment.owner == owner_node end)
        |> Enum.reduce({0, Policy.session_counts(assignments)}, fn {session_id, assignment},
                                                                   {acc, counts} ->
          case Policy.failover_target(assignment, owner_node, ready_nodes, counts) do
            nil ->
              {acc, counts}

            target_node ->
              case route_command(
                     :failover_assignment,
                     [session_id, assignment, target_node, failed_at_ms]
                   ) do
                :ok -> {acc + 1, Map.update(counts, target_node, 1, &(&1 + 1))}
                {:error, _reason} -> {acc, counts}
              end
          end
        end)

      {:ok, moved}
    end
  end

  @spec all_assignments(:low_latency | :consistency) ::
          {:ok, %{String.t() => assignment()}} | {:error, term()}
  def all_assignments(favor \\ :low_latency) when favor in [:low_latency, :consistency] do
    do_all_assignments(favor)
  end

  @spec all_node_records(:low_latency | :consistency) ::
          {:ok, %{node() => node_record()}} | {:error, term()}
  def all_node_records(favor \\ :low_latency) when favor in [:low_latency, :consistency] do
    do_all_node_records(favor)
  end

  @spec mark_node_draining(node()) :: :ok | {:error, term()}
  def mark_node_draining(node) when is_atom(node) do
    put_node_state(node, :draining)
  end

  @spec mark_node_ready(node()) :: :ok | {:error, term()}
  def mark_node_ready(node) when is_atom(node) do
    put_node_state(node, :ready)
  end

  @spec mark_node_drained(node()) :: :ok | {:error, term()}
  def mark_node_drained(node) when is_atom(node) do
    put_node_state(node, :drained)
  end

  @spec node_status(node()) :: :ready | :draining | :drained | :unknown
  def node_status(node) when is_atom(node) do
    case do_get_node_state(node, :low_latency) do
      {:ok, %{status: status}} when status in [:ready, :draining, :drained] -> status
      _other -> :unknown
    end
  end

  @spec node_record(node(), keyword()) :: {:ok, node_record()} | {:error, term()}
  def node_record(node, opts \\ []) when is_atom(node) and is_list(opts) do
    do_get_node_state(node, Keyword.get(opts, :favor, :low_latency))
  end

  @spec renew_local_lease() :: :ok | {:error, term()}
  def renew_local_lease do
    route_command(:renew_node_lease, [Node.self(), now_ms(), lease_ttl_ms()])
  end

  @spec expired_nodes(non_neg_integer()) :: [node()]
  def expired_nodes(now_ms \\ now_ms()) when is_integer(now_ms) and now_ms >= 0 do
    case all_node_records(:consistency) do
      {:ok, records} ->
        records
        |> Enum.filter(fn {_node, record} -> lease_expired?(record, now_ms) end)
        |> Enum.map(fn {node, _record} -> node end)

      {:error, _reason} ->
        []
    end
  end

  @spec ready_nodes() :: [node()]
  def ready_nodes do
    now_ms = now_ms()

    case all_node_records(:low_latency) do
      {:ok, records} ->
        records
        |> eligible_ready_nodes(now_ms)
        |> Enum.map(fn {node, _record} -> node end)
        |> Enum.sort()

      {:error, _reason} ->
        []
    end
  end

  @impl true
  def init(_arg) do
    Process.flag(:trap_exit, true)
    :ok = :net_kernel.monitor_nodes(true, node_type: :visible)
    :ok = start_store()
    :ok = join_cluster()
    :ok = :khepri_cluster.wait_for_leader(store_id(), @join_timeout_ms)
    :ok = do_bootstrap_local_node_local(now_ms(), lease_ttl_ms())
    send(self(), :cluster_bootstrap)
    schedule_cluster_reconcile()
    {:ok, %{}}
  end

  @impl true
  def handle_call({:run_local, fun, args}, _from, state)
      when is_atom(fun) and is_list(args) do
    {:reply, apply(__MODULE__, :"do_#{fun}_local", args), state}
  end

  @impl true
  def handle_info(:cluster_bootstrap, state) do
    :ok = join_cluster()
    {:noreply, state}
  end

  def handle_info(:cluster_reconcile, state) do
    :ok = join_cluster()
    schedule_cluster_reconcile()
    {:noreply, state}
  end

  def handle_info({:nodeup, node, _info}, state) when is_atom(node) do
    {:noreply, join_known_node(node, state)}
  end

  def handle_info({:nodeup, node}, state) when is_atom(node) do
    {:noreply, join_known_node(node, state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp start_store do
    case :khepri.start(local_store_dir(), store_id()) do
      {:ok, _store_id} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        raise "failed to start routing store: #{inspect(reason)}"
    end
  end

  defp join_cluster do
    cond do
      cluster_bootstrap_node?() ->
        :ok

      cluster_joined?() ->
        :ok

      true ->
        case bootstrap_node() do
          nil ->
            :ok

          node ->
            case :khepri_cluster.join({store_id(), node}, @join_timeout_ms) do
              :ok ->
                :ok

              {:error, {:already_member, _member}} ->
                :ok

              {:error, {:not_joinable, _reason}} ->
                :ok

              {:error, {:failed_to_contact_remote_member, _reason}} ->
                :ok

              {:error, {:no_leader, _reason}} ->
                :ok

              {:error, reason} ->
                Logger.debug("Routing.Store join skipped: #{inspect(reason)}")
                :ok
            end
        end
    end
  end

  defp bootstrap_node do
    Topology.nodes() |> List.first()
  end

  defp cluster_bootstrap_node? do
    bootstrap_node() == Node.self()
  end

  defp cluster_joined? do
    case bootstrap_node() do
      nil ->
        true

      bootstrap ->
        if bootstrap == Node.self() do
          true
        else
          case :khepri_cluster.nodes(store_id(), %{favor: :low_latency}) do
            {:ok, nodes} when is_list(nodes) ->
              Node.self() in nodes and bootstrap in nodes and length(nodes) > 1

            _other ->
              false
          end
        end
    end
  end

  defp schedule_cluster_reconcile do
    Process.send_after(self(), :cluster_reconcile, @cluster_reconcile_interval_ms)
  end

  defp join_known_node(node, state) when is_atom(node) and is_map(state) do
    if node in Topology.nodes() do
      :ok = join_cluster()
    end

    state
  end

  defp do_get_assignment(session_id, favor)
       when is_binary(session_id) and session_id != "" and favor in [:low_latency, :consistency] do
    with {:ok, result} <- local_call(:get_assignment, [session_id, favor]) do
      assignment_from_khepri(result)
    end
  end

  def do_get_assignment_local(session_id, favor)
      when is_binary(session_id) and session_id != "" and favor in [:low_latency, :consistency] do
    :khepri.get(store_id(), session_path(session_id), %{favor: favor})
  end

  def do_claim_assignment_local(session_id, claimed_at_ms)
      when is_binary(session_id) and session_id != "" and is_integer(claimed_at_ms) and
             claimed_at_ms >= 0 do
    case do_get_assignment_local(session_id, :consistency) do
      {:ok, assignment} when is_map(assignment) ->
        {:ok, assignment}

      {:error, reason} ->
        if missing_node_error?(reason) do
          claim_new_assignment(session_id, claimed_at_ms)
        else
          {:error, reason}
        end
    end
  end

  defp compare_and_swap_assignment(session_id, current, next)
       when is_binary(session_id) and is_map(current) and is_map(next) do
    route_command(:compare_and_swap_assignment, [session_id, current, next])
  end

  def do_compare_and_swap_assignment_local(session_id, current, next)
      when is_binary(session_id) and is_map(current) and is_map(next) do
    case :khepri.compare_and_swap(
           store_id(),
           session_path(session_id),
           exact_match_pattern(current),
           next,
           command_options()
         ) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def do_commit_transfer_local(session_id, transfer_id, committed_at_ms)
      when is_binary(session_id) and session_id != "" and is_binary(transfer_id) and
             transfer_id != "" and is_integer(committed_at_ms) and committed_at_ms >= 0 do
    with {:ok, current} <-
           assignment_from_khepri(do_get_assignment_local(session_id, :consistency)),
         :ok <- ensure_matching_transfer(current, transfer_id),
         {:ok, node_records} <- node_records_from_khepri(do_all_node_records_local(:consistency)) do
      next =
        activate_assignment(
          current,
          current.target_owner,
          ready_node_list(node_records, committed_at_ms),
          committed_at_ms
        )

      case :khepri.compare_and_swap(
             store_id(),
             session_path(session_id),
             exact_match_pattern(current),
             next,
             command_options()
           ) do
        :ok -> {:ok, next}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp do_all_assignments(favor) when favor in [:low_latency, :consistency] do
    with {:ok, result} <- local_call(:all_assignments, [favor]),
         {:ok, assignments} <- assignments_from_khepri(result) do
      {:ok, assignments}
    end
  end

  def do_all_assignments_local(favor) when favor in [:low_latency, :consistency] do
    :khepri.get_many(store_id(), "/:sessions/*", %{favor: favor})
  end

  defp do_all_node_records(favor) when favor in [:low_latency, :consistency] do
    with {:ok, result} <- local_call(:all_node_records, [favor]),
         {:ok, node_records} <- node_records_from_khepri(result) do
      {:ok, node_records}
    end
  end

  def do_all_node_records_local(favor) when favor in [:low_latency, :consistency] do
    :khepri.get_many(store_id(), "/:nodes/*", %{favor: favor})
  end

  defp assignments_from_khepri({:ok, payloads}) when is_map(payloads) do
    assignments =
      for {[:sessions, session_id], assignment} <- payloads,
          is_binary(session_id) and is_map(assignment),
          into: %{} do
        {session_id, assignment}
      end

    {:ok, assignments}
  end

  defp assignments_from_khepri({:error, reason}) do
    if missing_node_error?(reason), do: {:ok, %{}}, else: {:error, reason}
  end

  defp assignments_from_khepri(other), do: {:error, {:invalid_assignments, other}}

  defp claim_new_assignment(session_id, claimed_at_ms)
       when is_binary(session_id) and session_id != "" and is_integer(claimed_at_ms) and
              claimed_at_ms >= 0 do
    with {:ok, assignments} <- assignments_from_khepri(do_all_assignments_local(:consistency)),
         {:ok, node_records} <- node_records_from_khepri(do_all_node_records_local(:consistency)),
         {:ok, [owner | _rest] = replicas} <-
           Policy.choose_claim_nodes(
             assignments,
             ready_node_list(node_records, claimed_at_ms),
             Topology.replication_factor()
           ) do
      create_assignment(
        session_id,
        %{
          owner: owner,
          epoch: 1,
          replicas: replicas,
          status: :active,
          updated_at_ms: claimed_at_ms
        }
      )
    end
  end

  defp create_assignment(session_id, assignment)
       when is_binary(session_id) and session_id != "" and is_map(assignment) do
    case :khepri.create(store_id(), session_path(session_id), assignment, command_options()) do
      :ok ->
        {:ok, assignment}

      {:error, reason} ->
        if mismatching_node_error?(reason) do
          assignment_from_khepri(do_get_assignment_local(session_id, :consistency))
        else
          {:error, reason}
        end
    end
  end

  defp node_records_from_khepri({:ok, payloads}) when is_map(payloads) do
    node_records =
      Enum.reduce(payloads, %{}, fn
        {[:nodes, raw_node], record}, acc when is_binary(raw_node) and is_map(record) ->
          case decode_node_name(raw_node) do
            {:ok, node} -> Map.put(acc, node, record)
            :error -> acc
          end

        _other, acc ->
          acc
      end)

    {:ok, node_records}
  end

  defp node_records_from_khepri({:error, reason}) do
    if missing_node_error?(reason), do: {:ok, %{}}, else: {:error, reason}
  end

  defp node_records_from_khepri(other), do: {:error, {:invalid_node_records, other}}

  defp put_node_state(node, status)
       when is_atom(node) and status in [:ready, :draining, :drained] do
    route_command(:put_node_state, [node, status, now_ms()])
  end

  def do_put_node_state_local(node, status, updated_at_ms)
      when is_atom(node) and status in [:ready, :draining, :drained] and
             is_integer(updated_at_ms) and updated_at_ms >= 0 do
    mutate_node_record(node, updated_at_ms, fn current -> %{current | status: status} end)
  end

  def do_bootstrap_local_node_local(updated_at_ms, ttl_ms)
      when is_integer(updated_at_ms) and updated_at_ms >= 0 and is_integer(ttl_ms) and ttl_ms > 0 do
    do_renew_node_lease_local(Node.self(), updated_at_ms, ttl_ms)
  end

  def do_renew_node_lease_local(node, updated_at_ms, ttl_ms)
      when is_atom(node) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
             is_integer(ttl_ms) and ttl_ms > 0 do
    lease_until_ms = updated_at_ms + ttl_ms

    mutate_node_record(node, updated_at_ms, fn current ->
      %{current | lease_until_ms: lease_until_ms}
    end)
  end

  def do_clear_local do
    with :ok <- :khepri.delete_many(store_id(), "/:sessions/*", command_options()),
         :ok <- :khepri.delete_many(store_id(), "/:nodes/*", command_options()),
         :ok <- do_bootstrap_local_node_local(now_ms(), lease_ttl_ms()) do
      :ok
    end
  end

  defp do_get_node_state(node, favor)
       when is_atom(node) and favor in [:low_latency, :consistency] do
    with {:ok, result} <- local_call(:get_node_state, [node, favor]) do
      case result do
        {:ok, state} when is_map(state) -> {:ok, state}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def do_get_node_state_local(node, favor)
      when is_atom(node) and favor in [:low_latency, :consistency] do
    :khepri.get(store_id(), node_path(node), %{favor: favor})
  end

  def do_failover_assignment_local(session_id, current, target_node, failed_at_ms)
      when is_binary(session_id) and session_id != "" and is_map(current) and is_atom(target_node) and
             is_integer(failed_at_ms) and failed_at_ms >= 0 do
    with {:ok, node_records} <- node_records_from_khepri(do_all_node_records_local(:consistency)) do
      next =
        activate_assignment(
          current,
          target_node,
          ready_node_list(node_records, failed_at_ms),
          failed_at_ms
        )

      case :khepri.compare_and_swap(
             store_id(),
             session_path(session_id),
             exact_match_pattern(current),
             next,
             command_options()
           ) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp route_command(fun, args) when is_atom(fun) and is_list(args) do
    case local_call(fun, args) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  defp local_call(fun, args) when is_atom(fun) and is_list(args) do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        try do
          {:ok, GenServer.call(__MODULE__, {:run_local, fun, args}, @join_timeout_ms)}
        catch
          :exit, _reason -> {:error, :routing_store_unavailable}
        end

      nil ->
        {:error, :routing_store_unavailable}
    end
  end

  defp mutate_node_record(node, updated_at_ms, updater, attempt \\ 0)
       when is_atom(node) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
              is_function(updater, 1) and is_integer(attempt) and attempt >= 0 do
    if attempt >= @cas_retry_limit do
      {:error, :routing_store_unavailable}
    else
      case do_get_node_state_local(node, :consistency) do
        {:ok, current} when is_map(current) ->
          next = updater.(current) |> Map.put(:updated_at_ms, updated_at_ms)

          case :khepri.compare_and_swap(
                 store_id(),
                 node_path(node),
                 exact_match_pattern(current),
                 next,
                 command_options()
               ) do
            :ok ->
              :ok

            {:error, reason} ->
              if mismatching_node_error?(reason) do
                mutate_node_record(node, updated_at_ms, updater, attempt + 1)
              else
                {:error, reason}
              end
          end

        {:error, reason} ->
          if missing_node_error?(reason) do
            next = updater.(default_node_record()) |> Map.put(:updated_at_ms, updated_at_ms)

            case :khepri.create(store_id(), node_path(node), next, command_options()) do
              :ok ->
                :ok

              {:error, reason} ->
                if mismatching_node_error?(reason) do
                  mutate_node_record(node, updated_at_ms, updater, attempt + 1)
                else
                  {:error, reason}
                end
            end
          else
            {:error, reason}
          end
      end
    end
  end

  defp ensure_matching_transfer(
         %{status: :moving, target_owner: target_owner, transfer_id: transfer_id},
         transfer_id
       )
       when is_atom(target_owner) and is_binary(transfer_id),
       do: :ok

  defp ensure_matching_transfer(_current, _transfer_id), do: {:error, :mismatching_node}

  defp assignment_from_khepri({:ok, assignment}) when is_map(assignment), do: {:ok, assignment}

  defp assignment_from_khepri({:error, reason}) do
    if missing_node_error?(reason), do: {:error, :not_found}, else: {:error, reason}
  end

  defp assignment_from_khepri(other), do: {:error, {:invalid_assignment_lookup, other}}

  defp eligible_ready_nodes(node_records, now_ms)
       when is_map(node_records) and is_integer(now_ms) and now_ms >= 0 do
    Enum.filter(node_records, fn {_node, record} ->
      record.status == :ready and not lease_expired?(record, now_ms)
    end)
  end

  defp ready_node_list(node_records, now_ms)
       when is_map(node_records) and is_integer(now_ms) and now_ms >= 0 do
    node_records
    |> eligible_ready_nodes(now_ms)
    |> Enum.map(fn {node, _record} -> node end)
  end

  defp activate_assignment(assignment, owner, ready_nodes, updated_at_ms)
       when is_map(assignment) and is_atom(owner) and is_list(ready_nodes) and
              is_integer(updated_at_ms) and updated_at_ms >= 0 do
    assignment
    |> Map.put(:owner, owner)
    |> Map.put(:epoch, assignment.epoch + 1)
    |> Map.put(:status, :active)
    |> Map.put(:updated_at_ms, updated_at_ms)
    |> Map.put(
      :replicas,
      Policy.rebalance_replicas(
        assignment.replicas,
        owner,
        ready_nodes,
        Topology.replication_factor()
      )
    )
    |> Map.delete(:target_owner)
    |> Map.delete(:transfer_id)
  end

  defp new_transfer_id do
    "xfer-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp session_path(session_id), do: [:sessions, session_id]
  defp node_path(node), do: [:nodes, Atom.to_string(node)]

  defp default_node_record do
    %{status: :ready, lease_until_ms: 0, updated_at_ms: 0}
  end

  @spec exact_match_pattern(term()) :: :ets.match_pattern()
  defp exact_match_pattern(value), do: value

  defp lease_expired?(record, now_ms)
       when is_map(record) and is_integer(now_ms) and now_ms >= 0 do
    record.lease_until_ms <= now_ms
  end

  defp decode_node_name(raw_node) when is_binary(raw_node) do
    case Enum.find(Topology.nodes(), fn node -> Atom.to_string(node) == raw_node end) do
      nil -> :error
      node -> {:ok, node}
    end
  end

  defp missing_node_error?({:khepri, :node_not_found, _info}), do: true
  defp missing_node_error?({:node_not_found, _info}), do: true
  defp missing_node_error?(_reason), do: false

  defp mismatching_node_error?({:khepri, :mismatching_node, _info}), do: true
  defp mismatching_node_error?({:mismatching_node, _info}), do: true
  defp mismatching_node_error?(:mismatching_node), do: true
  defp mismatching_node_error?(_reason), do: false

  defp lease_ttl_ms do
    case Application.get_env(:starcite, :routing_lease_ttl_ms, 5_000) do
      value when is_integer(value) and value > 0 -> value
      value -> raise ArgumentError, "invalid value for :routing_lease_ttl_ms: #{inspect(value)}"
    end
  end

  defp command_options do
    %{
      async: false,
      reply_from: :local,
      timeout: @join_timeout_ms
    }
  end

  defp sanitize_node_name(node) when is_atom(node) do
    node
    |> Atom.to_string()
    |> String.replace(~r/[^a-zA-Z0-9_.-]+/, "_")
  end

  defp now_ms, do: System.system_time(:millisecond)
end
