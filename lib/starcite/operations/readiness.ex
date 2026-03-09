defmodule Starcite.Operations.Readiness do
  @moduledoc false

  alias Starcite.Routing.{Store, Topology}

  @default_wait_interval_ms 200

  @spec local_ready(keyword()) :: boolean()
  def local_ready(opts \\ []) when is_list(opts) do
    local_readiness(opts).ready?
  end

  @spec local_readiness(keyword()) :: map()
  def local_readiness(_opts \\ []) do
    mode = local_mode()

    cond do
      mode == :ingress_node ->
        ready_result(%{routing_store: %{ready?: true, reason: :ok, detail: %{}}})

      local_drained() ->
        not_ready_result(:draining, %{}, %{
          routing_store: %{ready?: false, reason: :draining, detail: %{}}
        })

      Store.running?() ->
        ready_result(%{routing_store: %{ready?: true, reason: :ok, detail: %{}}})

      true ->
        not_ready_result(:routing_sync, %{}, %{
          routing_store: %{ready?: false, reason: :routing_sync, detail: %{}}
        })
    end
  end

  @spec local_drained() :: boolean()
  def local_drained do
    local_mode() == :routing_node and Store.node_status(Node.self()) == :draining
  end

  @spec wait_local_ready(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_ready(timeout_ms \\ 30_000)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    wait_until(fn -> local_ready(refresh?: true) end, timeout_ms)
  end

  @spec wait_local_drained(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_drained(timeout_ms \\ 30_000)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    wait_until(&local_drained/0, timeout_ms)
  end

  defp local_mode do
    if Topology.routing_node?(Node.self()), do: :routing_node, else: :ingress_node
  end

  defp ready_result(checks) when is_map(checks) do
    %{ready?: true, reason: :ok, detail: %{}, checks: checks}
  end

  defp not_ready_result(reason, detail, checks)
       when is_atom(reason) and is_map(detail) and is_map(checks) do
    %{ready?: false, reason: reason, detail: detail, checks: checks}
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
