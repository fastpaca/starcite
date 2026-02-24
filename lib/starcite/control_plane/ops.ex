defmodule Starcite.ControlPlane.Ops do
  @moduledoc """
  Operator-facing helpers for static write-node control and status.

  These helpers affect routing eligibility only. They do not mutate Raft
  membership.
  """

  alias Starcite.ControlPlane.Observer
  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.DataPlane.{RaftBootstrap, RaftManager}

  @default_wait_interval_ms 200
  @status_call_timeout_ms 1_000

  @spec status() :: map()
  def status do
    local_mode = local_mode()

    %{
      node: Node.self(),
      local_write_node: WriteNodes.write_node?(Node.self()),
      local_mode: local_mode,
      local_ready: local_ready(),
      local_drained: local_drained(),
      write_nodes: WriteNodes.nodes(),
      write_replication_factor: WriteNodes.replication_factor(),
      num_groups: WriteNodes.num_groups(),
      local_groups: local_write_groups(),
      observer: Observer.status()
    }
  end

  @spec ready_nodes() :: [node()]
  def ready_nodes do
    Observer.ready_nodes()
  end

  @spec local_mode() :: :write_node | :router_node
  def local_mode do
    if WriteNodes.write_node?(Node.self()) do
      :write_node
    else
      :router_node
    end
  end

  @spec local_ready(keyword()) :: boolean()
  def local_ready(opts \\ []) when is_list(opts) do
    local_readiness(opts).ready?
  end

  @spec local_readiness(keyword()) :: map()
  def local_readiness(opts \\ []) when is_list(opts) do
    mode = local_mode()
    refresh? = Keyword.get(opts, :refresh?, false)
    checks = readiness_checks(mode, refresh?)

    compose_readiness(mode, checks)
  end

  @spec local_drained() :: boolean()
  def local_drained do
    local_mode() == :write_node and
      local_node_status() == :draining and
      Node.self() not in Observer.ready_nodes()
  end

  @spec wait_local_ready(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_ready(timeout_ms \\ 30_000)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    wait_until(fn -> local_ready(refresh?: true) end, timeout_ms)
  end

  @spec wait_local_drained(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_drained(timeout_ms \\ 30_000)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    wait_until(&local_drain_complete/0, timeout_ms)
  end

  @spec drain_node(node()) :: :ok | {:error, :invalid_write_node}
  def drain_node(node \\ Node.self()) when is_atom(node) do
    with :ok <- ensure_write_node(node) do
      Observer.mark_node_draining(node)
    end
  end

  @spec undrain_node(node()) :: :ok | {:error, :invalid_write_node}
  def undrain_node(node \\ Node.self()) when is_atom(node) do
    with :ok <- ensure_write_node(node) do
      Observer.mark_node_ready(node)
    end
  end

  @spec local_write_groups() :: [non_neg_integer()]
  def local_write_groups do
    for group_id <- 0..(WriteNodes.num_groups() - 1),
        RaftManager.should_participate?(group_id),
        do: group_id
  end

  @spec group_replicas(non_neg_integer()) :: [node()]
  def group_replicas(group_id) when is_integer(group_id) and group_id >= 0 do
    RaftManager.replicas_for_group(group_id)
  end

  @spec known_nodes() :: [node()]
  def known_nodes do
    WriteNodes.nodes()
  end

  @spec parse_known_node(term()) :: {:ok, node()} | {:error, :invalid_write_node}
  def parse_known_node(raw_node) when is_binary(raw_node) do
    node_name = String.trim(raw_node)
    known_nodes = known_nodes()

    case Enum.find(known_nodes, fn node -> Atom.to_string(node) == node_name end) do
      nil -> {:error, :invalid_write_node}
      node -> {:ok, node}
    end
  end

  def parse_known_node(_raw_node), do: {:error, :invalid_write_node}

  @spec parse_group_id(term()) :: {:ok, non_neg_integer()} | {:error, :invalid_group_id}
  def parse_group_id(raw_group_id) when is_binary(raw_group_id) do
    case Integer.parse(String.trim(raw_group_id)) do
      {group_id, ""} when group_id >= 0 ->
        parse_group_id(group_id)

      _ ->
        {:error, :invalid_group_id}
    end
  end

  def parse_group_id(group_id) when is_integer(group_id) and group_id >= 0 do
    if group_id < WriteNodes.num_groups() do
      {:ok, group_id}
    else
      {:error, :invalid_group_id}
    end
  end

  def parse_group_id(_raw_group_id), do: {:error, :invalid_group_id}

  defp ensure_write_node(node) when is_atom(node) do
    if WriteNodes.write_node?(node) do
      :ok
    else
      {:error, :invalid_write_node}
    end
  end

  defp local_node_status do
    local = Node.self()

    case Observer.status() do
      %{status: :ok, node_statuses: %{^local => %{status: status}}} when is_atom(status) ->
        status

      _other ->
        nil
    end
  end

  defp readiness_checks(mode, refresh?)
       when mode in [:write_node, :router_node] and is_boolean(refresh?) do
    %{
      in_service: in_service_gate(mode),
      write_path: write_path_gate(refresh?)
    }
  end

  defp in_service_gate(:router_node) do
    %{
      ready?: true,
      reason: :ok,
      detail: %{}
    }
  end

  defp in_service_gate(:write_node) do
    cond do
      local_drained() ->
        %{
          ready?: false,
          reason: :draining,
          detail: %{}
        }

      Node.self() in Observer.ready_nodes() ->
        %{
          ready?: true,
          reason: :ok,
          detail: %{}
        }

      true ->
        %{
          ready?: false,
          reason: :observer_sync,
          detail: %{}
        }
    end
  end

  defp write_path_gate(refresh?) when is_boolean(refresh?) do
    RaftBootstrap.readiness_status(refresh?: refresh?)
  end

  defp compose_readiness(:router_node, %{write_path: write_path} = checks) when is_map(checks) do
    if write_path.ready? do
      ready_result(checks)
    else
      not_ready_result(write_path.reason, write_path.detail, checks)
    end
  end

  defp compose_readiness(:write_node, %{in_service: in_service, write_path: write_path} = checks)
       when is_map(checks) do
    # Keep gate precedence explicit: operator drain first, then write-path safety, then observer routing.
    cond do
      in_service.reason == :draining ->
        not_ready_result(:draining, %{}, checks)

      not write_path.ready? ->
        not_ready_result(write_path.reason, write_path.detail, checks)

      not in_service.ready? ->
        not_ready_result(:observer_sync, %{}, checks)

      true ->
        ready_result(checks)
    end
  end

  defp ready_result(checks) when is_map(checks) do
    %{
      ready?: true,
      reason: :ok,
      detail: %{},
      checks: checks
    }
  end

  defp not_ready_result(reason, detail, checks)
       when is_atom(reason) and is_map(detail) and is_map(checks) do
    %{
      ready?: false,
      reason: reason,
      detail: detail,
      checks: checks
    }
  end

  defp local_drain_complete do
    local = Node.self()

    local_mode() == :write_node and
      local_drained() and
      drained_on_visible_nodes?(local)
  end

  defp drained_on_visible_nodes?(node) when is_atom(node) do
    Observer.all_nodes()
    |> Enum.all?(fn observer_node ->
      observer_node_status(observer_node, node) == :draining
    end)
  end

  defp observer_node_status(observer_node, target_node)
       when is_atom(observer_node) and is_atom(target_node) do
    status =
      if observer_node == Node.self() do
        Observer.status()
      else
        :rpc.call(observer_node, Observer, :status, [], @status_call_timeout_ms)
      end

    case status do
      %{status: :ok, node_statuses: %{^target_node => %{status: node_status}}}
      when is_atom(node_status) ->
        node_status

      _other ->
        nil
    end
  end

  defp wait_until(predicate, timeout_ms)
       when is_function(predicate, 0) and is_integer(timeout_ms) and timeout_ms > 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(predicate, deadline_ms)
  end

  defp do_wait_until(predicate, deadline_ms)
       when is_function(predicate, 0) and is_integer(deadline_ms) do
    if predicate.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline_ms do
        {:error, :timeout}
      else
        Process.sleep(@default_wait_interval_ms)
        do_wait_until(predicate, deadline_ms)
      end
    end
  end
end
