defmodule Starcite.DataPlane.RaftBootstrapTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.RaftBootstrap

  test "startup_complete is idempotent once startup has finished" do
    original_state = :sys.get_state(RaftBootstrap)

    on_exit(fn ->
      :sys.replace_state(RaftBootstrap, fn _state -> original_state end)
    end)

    now_ms = System.monotonic_time(:millisecond)

    :sys.replace_state(RaftBootstrap, fn state ->
      state
      |> Map.put(:startup_complete?, true)
      |> Map.put(:startup_mode, :coordinator)
      |> Map.put(:consensus_ready?, true)
      |> Map.put(:consensus_last_probe_at_ms, now_ms)
      |> Map.put(:consensus_probe_detail, %{
        checked_groups: 1,
        failing_group_id: nil,
        probe_result: "ok"
      })
    end)

    send(RaftBootstrap, {:startup_complete, :follower})

    eventually(fn ->
      assert %{
               startup_complete?: true,
               startup_mode: :follower,
               consensus_ready?: true,
               consensus_last_probe_at_ms: ^now_ms,
               consensus_probe_detail: %{probe_result: "ok"}
             } = :sys.get_state(RaftBootstrap)
    end)
  end

  defp eventually(fun, opts \\ []) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 1_000)
    interval = Keyword.get(opts, :interval, 25)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    try do
      fun.()
    rescue
      error in [ExUnit.AssertionError] ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        else
          reraise(error, __STACKTRACE__)
        end
    end
  end
end
