defmodule FleetLM.TestCase do
  @moduledoc """
  Lean ExUnit case template for FleetLM.

  Provides a clean runtime after each test and a small set of helpers tailored
  for the Raft-only runtime.
  """

  use ExUnit.CaseTemplate

  using _opts do
    quote do
      import FleetLM.TestCase
    end
  end

  setup _tags do
    previous_config = configure_test_env()

    on_exit(fn ->
      FleetLM.Runtime.TestHelper.reset()
      restore_config(previous_config)
    end)

    {:ok, %{}}
  end

  @doc """
  Retry the provided assertion until it succeeds or the timeout elapses.
  Re-raises the last assertion error when the timeout is exceeded.
  """
  def eventually(fun, opts \\ []) when is_function(fun, 0) do
    timeout = Keyword.get(opts, :timeout, 1_000)
    interval = Keyword.get(opts, :interval, 25)
    deadline = System.monotonic_time(:millisecond) + timeout

    try_eventually(fun, interval, deadline)
  end

  defp configure_test_env do
    previous = %{
      disable_agent_webhooks: Application.get_env(:fleet_lm, :disable_agent_webhooks),
      agent_dispatch_tick_ms: Application.get_env(:fleet_lm, :agent_dispatch_tick_ms),
      agent_debounce_window_ms: Application.get_env(:fleet_lm, :agent_debounce_window_ms)
    }

    Application.put_env(:fleet_lm, :disable_agent_webhooks, true)
    Application.put_env(:fleet_lm, :agent_dispatch_tick_ms, 10)
    Application.put_env(:fleet_lm, :agent_debounce_window_ms, 0)

    previous
  end

  defp restore_config(previous) do
    Enum.each(previous, fn
      {key, nil} -> Application.delete_env(:fleet_lm, key)
      {key, value} -> Application.put_env(:fleet_lm, key, value)
    end)
  end

  defp try_eventually(fun, interval, deadline) do
    fun.()
    :ok
  rescue
    error in [ExUnit.AssertionError] ->
      if System.monotonic_time(:millisecond) >= deadline do
        reraise(error, __STACKTRACE__)
      else
        Process.sleep(interval)
        try_eventually(fun, interval, deadline)
      end
  end
end
