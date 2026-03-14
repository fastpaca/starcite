defmodule Starcite.Operations.Readiness do
  @moduledoc false

  alias Starcite.Routing.{Store, Watcher}

  @default_wait_interval_ms 200

  @spec local_ready(keyword()) :: boolean()
  def local_ready(opts \\ []) when is_list(opts) do
    local_readiness(opts).ready?
  end

  @spec local_readiness(keyword()) :: map()
  def local_readiness(_opts \\ []) do
    now_ms = System.system_time(:millisecond)

    cond do
      not Store.running?() ->
        not_ready_result(:routing_sync, %{}, %{
          routing_store: %{ready?: false, reason: :routing_sync, detail: %{}}
        })

      true ->
        local_routing_readiness(now_ms)
    end
  end

  defp local_routing_readiness(now_ms) when is_integer(now_ms) and now_ms >= 0 do
    case Store.node_record(Node.self(), favor: :consistency) do
      {:ok, %{status: :ready, lease_until_ms: lease_until_ms}}
      when is_integer(lease_until_ms) and lease_until_ms > now_ms ->
        ready_result(%{
          routing_store: %{
            ready?: true,
            reason: :ok,
            detail: %{status: :ready, lease_until_ms: lease_until_ms}
          }
        })

      {:ok, %{status: status}} when status in [:draining, :drained] ->
        not_ready_result(:draining, %{status: status}, %{
          routing_store: %{
            ready?: false,
            reason: :draining,
            detail: %{status: status}
          }
        })

      {:ok, %{status: :ready, lease_until_ms: lease_until_ms}}
      when is_integer(lease_until_ms) ->
        not_ready_result(:lease_expired, %{lease_until_ms: lease_until_ms}, %{
          routing_store: %{
            ready?: false,
            reason: :lease_expired,
            detail: %{status: :ready, lease_until_ms: lease_until_ms}
          }
        })

      _other ->
        not_ready_result(:routing_sync, %{}, %{
          routing_store: %{ready?: false, reason: :routing_sync, detail: %{status: :unknown}}
        })
    end
  end

  @spec local_drained() :: boolean()
  def local_drained, do: drain_complete?(Node.self())

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

  defp drain_complete?(node) when is_atom(node) do
    progress_local_drain(node)

    with status when status in [:draining, :drained] <- Store.node_status(node),
         {:ok, %{active_owned_sessions: 0, moving_sessions: 0}} <- Store.drain_status(node) do
      true
    else
      _other -> false
    end
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
    cond do
      predicate.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline_ms ->
        {:error, :timeout}

      true ->
        Process.sleep(@default_wait_interval_ms)
        do_wait_until(predicate, deadline_ms)
    end
  end

  defp progress_local_drain(node) when is_atom(node) do
    if node == Node.self() do
      _ = Watcher.run_once()
    end

    :ok
  end
end
