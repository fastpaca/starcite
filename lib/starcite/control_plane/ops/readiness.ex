defmodule Starcite.ControlPlane.Ops.Readiness do
  @moduledoc false

  alias Starcite.ControlPlane.Observer
  alias Starcite.ControlPlane.Ops.Topology
  alias Starcite.ControlPlane.RaftBootstrap

  @default_wait_interval_ms 200
  @status_call_timeout_ms 1_000

  @spec local_ready(keyword()) :: boolean()
  def local_ready(opts \\ []) when is_list(opts) do
    local_readiness(opts).ready?
  end

  @spec local_readiness(keyword()) :: map()
  def local_readiness(opts \\ []) when is_list(opts) do
    mode = Topology.local_mode()
    refresh? = Keyword.get(opts, :refresh?, false)
    checks = readiness_checks(mode, refresh?)

    compose_readiness(mode, checks)
  end

  @spec local_drained() :: boolean()
  def local_drained do
    Topology.local_mode() == :write_node and
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
      write_path: RaftBootstrap.readiness_status(refresh?: refresh?)
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

  defp compose_readiness(:router_node, %{write_path: write_path} = checks) when is_map(checks) do
    if write_path.ready? do
      ready_result(checks)
    else
      not_ready_result(write_path.reason, write_path.detail, checks)
    end
  end

  defp compose_readiness(:write_node, %{in_service: in_service, write_path: write_path} = checks)
       when is_map(checks) do
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

    Topology.local_mode() == :write_node and
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
