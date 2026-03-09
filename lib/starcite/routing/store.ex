defmodule Starcite.Routing.Store do
  @moduledoc """
  Khepri-backed routing ownership store.

  The routing store is the durable control-plane record for active sessions:

  - `session_id -> %{owner, epoch, replicas, status}`
  - node drain state under `[:nodes, node_name]`

  It is intentionally small. The data plane owns sequencing and
  replication; this store only answers who owns a session and which epoch that
  ownership is fenced with.
  """

  use GenServer

  require Logger

  alias Starcite.Routing.Topology

  @join_timeout_ms 5_000
  @default_store_id :starcite_routing
  @default_store_dir "priv/khepri"

  @type assignment :: %{
          required(:owner) => node(),
          required(:epoch) => non_neg_integer(),
          required(:replicas) => [node()],
          required(:status) => :active | :moving,
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
        assignment = initial_assignment(session_id)

        case do_create_assignment(session_id, assignment) do
          :ok ->
            {:ok, assignment}

          {:error, _reason} ->
            do_get_assignment(session_id, :consistency)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec transfer_session(String.t(), node()) :: :ok | {:error, term()}
  def transfer_session(session_id, target_node)
      when is_binary(session_id) and session_id != "" and is_atom(target_node) do
    with {:ok, current} <- do_get_assignment(session_id, :consistency),
         :ok <- ensure_routable_node(target_node) do
      next =
        current
        |> Map.put(:owner, target_node)
        |> Map.put(:epoch, current.epoch + 1)
        |> Map.put(:updated_at_ms, now_ms())
        |> Map.put(:replicas, rebalance_replicas(session_id, current.replicas, target_node))

      compare_and_swap_assignment(session_id, current, next)
    end
  end

  @spec reassign_sessions_from(node()) :: {:ok, non_neg_integer()} | {:error, term()}
  def reassign_sessions_from(owner_node) when is_atom(owner_node) do
    with {:ok, assignments} <- all_assignments(:consistency) do
      moved =
        assignments
        |> Enum.filter(fn {_session_id, assignment} -> assignment.owner == owner_node end)
        |> Enum.reduce(0, fn {session_id, assignment}, acc ->
          case next_owner(assignment, owner_node) do
            nil ->
              acc

            target_node ->
              next =
                assignment
                |> Map.put(:owner, target_node)
                |> Map.put(:epoch, assignment.epoch + 1)
                |> Map.put(:status, :active)
                |> Map.put(:updated_at_ms, now_ms())
                |> Map.put(
                  :replicas,
                  rebalance_replicas(session_id, assignment.replicas, target_node)
                )

              case compare_and_swap_assignment(session_id, assignment, next) do
                :ok -> acc + 1
                {:error, _reason} -> acc
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

  @spec mark_node_draining(node()) :: :ok | {:error, term()}
  def mark_node_draining(node) when is_atom(node) do
    put_node_state(node, :draining)
  end

  @spec mark_node_ready(node()) :: :ok | {:error, term()}
  def mark_node_ready(node) when is_atom(node) do
    put_node_state(node, :ready)
  end

  @spec node_status(node()) :: :ready | :draining | :unknown
  def node_status(node) when is_atom(node) do
    case do_get_node_state(node, :low_latency) do
      {:ok, %{status: status}} when status in [:ready, :draining] -> status
      _other -> :unknown
    end
  end

  @spec ready_nodes() :: [node()]
  def ready_nodes do
    Topology.nodes()
    |> Enum.filter(fn node -> node_status(node) != :draining end)
  end

  @impl true
  def init(_arg) do
    Process.flag(:trap_exit, true)

    if Topology.routing_node?(Node.self()) do
      :net_kernel.monitor_nodes(true, node_type: :visible)
      :ok = start_store()
      :ok = maybe_join_cluster()
      :ok = wait_for_leader()
      :ok = do_put_node_state_local(Node.self(), :ready)
      {:ok, %{}}
    else
      {:ok, %{}}
    end
  end

  @impl true
  def handle_call({:run_local, fun, args}, _from, state)
      when is_atom(fun) and is_list(args) do
    {:reply, apply(__MODULE__, :"do_#{fun}_local", args), state}
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) when is_atom(node) do
    if Topology.routing_node?(node) do
      :ok = maybe_join_cluster()
    end

    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) when is_atom(node) do
    if Topology.routing_node?(Node.self()) and Topology.routing_node?(node) do
      _ = reassign_sessions_from(node)
    end

    {:noreply, state}
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

  defp maybe_join_cluster do
    if Topology.routing_node?(Node.self()) do
      case bootstrap_node() do
        nil ->
          :ok

        node ->
          if node == Node.self() do
            :ok
          else
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
    else
      :ok
    end
  end

  defp wait_for_leader do
    case :khepri_cluster.wait_for_leader(store_id(), @join_timeout_ms) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp bootstrap_node do
    Topology.nodes()
    |> Enum.find(fn node -> node == Node.self() or node in Node.list(:connected) end)
  end

  defp do_get_assignment(session_id, favor)
       when is_binary(session_id) and session_id != "" and favor in [:low_latency, :consistency] do
    with {:ok, result} <- route_query(:get_assignment, [session_id, favor]) do
      normalize_get_assignment(result)
    end
  end

  defp normalize_get_assignment({:ok, assignment}) when is_map(assignment),
    do: {:ok, normalize_assignment(assignment)}

  defp normalize_get_assignment({:error, reason}) do
    if missing_node_error?(reason), do: {:error, :not_found}, else: {:error, reason}
  end

  defp normalize_get_assignment(other), do: {:error, {:invalid_assignment_lookup, other}}

  def do_get_assignment_local(session_id, favor)
      when is_binary(session_id) and session_id != "" and favor in [:low_latency, :consistency] do
    :khepri.get(store_id(), session_path(session_id), %{favor: favor})
  end

  defp do_create_assignment(session_id, assignment)
       when is_binary(session_id) and session_id != "" and is_map(assignment) do
    route_command(:create_assignment, [session_id, assignment])
  end

  def do_create_assignment_local(session_id, assignment)
      when is_binary(session_id) and session_id != "" and is_map(assignment) do
    :khepri.create(store_id(), session_path(session_id), assignment, command_options())
  end

  defp compare_and_swap_assignment(session_id, current, next)
       when is_binary(session_id) and is_map(current) and is_map(next) do
    route_command(:compare_and_swap_assignment, [session_id, current, next])
  end

  def do_compare_and_swap_assignment_local(session_id, current, next)
      when is_binary(session_id) and is_map(current) and is_map(next) do
    path = session_path(session_id)

    case :khepri.transaction(
           store_id(),
           fn ->
             case :khepri_tx.get(path) do
               {:ok, value} when value == current ->
                 :ok = :khepri_tx.put(path, next)
                 :ok

               {:ok, _other} ->
                 :khepri_tx.abort(:mismatching_node)

               {:error, reason} ->
                 :khepri_tx.abort(reason)
             end
           end,
           command_options()
         ) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_all_assignments(favor) when favor in [:low_latency, :consistency] do
    with {:ok, result} <- route_query(:all_assignments, [favor]),
         {:ok, assignments} <- normalize_assignments(result) do
      {:ok, assignments}
    end
  end

  def do_all_assignments_local(favor) when favor in [:low_latency, :consistency] do
    :khepri.get_many(store_id(), "/:sessions/*", %{favor: favor})
  end

  defp normalize_assignments({:ok, payloads}) when is_map(payloads) do
    assignments =
      Enum.reduce(payloads, %{}, fn
        {[:sessions, session_id], assignment}, acc
        when is_binary(session_id) and is_map(assignment) ->
          Map.put(acc, session_id, normalize_assignment(assignment))

        _other, acc ->
          acc
      end)

    {:ok, assignments}
  end

  defp normalize_assignments({:error, reason}), do: {:error, reason}
  defp normalize_assignments(other), do: {:error, {:invalid_assignments, other}}

  defp put_node_state(node, status) when is_atom(node) and status in [:ready, :draining] do
    route_command(:put_node_state, [node, status])
  end

  def do_put_node_state_local(node, status)
      when is_atom(node) and status in [:ready, :draining] do
    :khepri.put(
      store_id(),
      node_path(node),
      %{status: status, updated_at_ms: now_ms()},
      command_options()
    )
  end

  def do_clear_local do
    with :ok <- :khepri.delete_many(store_id(), "/:sessions/*", command_options()),
         :ok <- :khepri.delete_many(store_id(), "/:nodes/*", command_options()),
         :ok <- do_put_node_state_local(Node.self(), :ready) do
      :ok
    end
  end

  defp do_get_node_state(node, favor)
       when is_atom(node) and favor in [:low_latency, :consistency] do
    case route_query(:get_node_state, [node, favor]) do
      {:ok, {:ok, state}} -> {:ok, state}
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  def do_get_node_state_local(node, favor)
      when is_atom(node) and favor in [:low_latency, :consistency] do
    :khepri.get(store_id(), node_path(node), %{favor: favor})
  end

  defp route_query(fun, args) when is_atom(fun) and is_list(args) do
    if Topology.routing_node?(Node.self()) do
      local_call(fun, args)
    else
      rpc_call(fun, args)
    end
  end

  defp route_command(fun, args) when is_atom(fun) and is_list(args) do
    if Topology.routing_node?(Node.self()) do
      case local_call(fun, args) do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    else
      case rpc_call(fun, args) do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
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

  defp rpc_call(fun, args) when is_atom(fun) and is_list(args) do
    Topology.nodes()
    |> Enum.reject(&(&1 == Node.self()))
    |> Enum.reduce_while({:error, :routing_store_unavailable}, fn node, _acc ->
      case :rpc.call(node, __MODULE__, fun, args, @join_timeout_ms) do
        {:badrpc, _reason} ->
          {:cont, {:error, :routing_store_unavailable}}

        result ->
          {:halt, {:ok, result}}
      end
    end)
  end

  defp ensure_routable_node(node) when is_atom(node) do
    if node in Topology.nodes() do
      :ok
    else
      {:error, :invalid_routing_node}
    end
  end

  defp initial_assignment(session_id) when is_binary(session_id) and session_id != "" do
    replicas = Topology.replicas_for_session(session_id, ready_candidate_nodes())

    %{
      owner: hd(replicas),
      epoch: 1,
      replicas: replicas,
      status: :active,
      updated_at_ms: now_ms()
    }
  end

  defp next_owner(%{replicas: replicas}, owner_node)
       when is_list(replicas) and is_atom(owner_node) do
    ready = ready_candidate_nodes()

    replicas
    |> Enum.reject(&(&1 == owner_node))
    |> Enum.find(&(&1 in ready))
  end

  defp rebalance_replicas(session_id, current_replicas, target_owner)
       when is_binary(session_id) and is_list(current_replicas) and is_atom(target_owner) do
    ready = ready_candidate_nodes()

    replicas =
      [target_owner | Enum.filter(current_replicas, &(&1 != target_owner and &1 in ready))]
      |> Enum.uniq()

    desired = Topology.replication_factor()

    if length(replicas) >= desired do
      Enum.take(replicas, desired)
    else
      additional =
        ready
        |> Enum.reject(&(&1 in replicas))
        |> then(&Topology.replicas_for_session(session_id, &1))
        |> Enum.reject(&(&1 in replicas))

      (replicas ++ additional)
      |> Enum.uniq()
      |> Enum.take(desired)
    end
  end

  defp ready_candidate_nodes do
    nodes = ready_nodes()
    if nodes == [], do: Topology.nodes(), else: nodes
  end

  defp session_path(session_id), do: [:sessions, session_id]
  defp node_path(node), do: [:nodes, Atom.to_string(node)]

  defp normalize_assignment(%{owner: owner, epoch: epoch, replicas: replicas} = assignment)
       when is_atom(owner) and is_integer(epoch) and epoch >= 0 and is_list(replicas) do
    assignment
  end

  defp normalize_assignment(assignment), do: assignment

  defp missing_node_error?({:khepri, :node_not_found, _info}), do: true
  defp missing_node_error?({:node_not_found, _info}), do: true
  defp missing_node_error?(_reason), do: false

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
