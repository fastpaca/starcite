defmodule Starcite.DataPlane.RaftBootstrapTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.RaftBootstrap

  test "schedules retry when sync result is not converged" do
    original_state = :sys.get_state(RaftBootstrap)

    on_exit(fn ->
      :sys.replace_state(RaftBootstrap, fn _state -> original_state end)
    end)

    sync_ref = make_ref()

    :sys.replace_state(RaftBootstrap, fn state ->
      state
      |> Map.put(:sync_ref, sync_ref)
      |> Map.put(:sync_retry_ref, nil)
    end)

    send(
      RaftBootstrap,
      {sync_ref,
       {:ok, :coordinator, false, %{probe_result: "leader_unknown", failing_group_id: 0}}}
    )

    eventually(fn ->
      assert %{sync_ref: nil, sync_retry_ref: retry_ref} = :sys.get_state(RaftBootstrap)
      assert is_reference(retry_ref)
    end)
  end

  test "clears pending retry timer when sync converges" do
    original_state = :sys.get_state(RaftBootstrap)

    on_exit(fn ->
      :sys.replace_state(RaftBootstrap, fn _state -> original_state end)
    end)

    first_ref = make_ref()

    :sys.replace_state(RaftBootstrap, fn state ->
      state
      |> Map.put(:sync_ref, first_ref)
      |> Map.put(:sync_retry_ref, nil)
    end)

    send(
      RaftBootstrap,
      {first_ref,
       {:ok, :coordinator, false, %{probe_result: "leader_unknown", failing_group_id: 7}}}
    )

    retry_ref =
      eventually(fn ->
        assert %{sync_ref: nil, sync_retry_ref: pending_retry_ref} =
                 :sys.get_state(RaftBootstrap)

        assert is_reference(pending_retry_ref)
        pending_retry_ref
      end)

    converged_ref = make_ref()

    :sys.replace_state(RaftBootstrap, fn state ->
      state
      |> Map.put(:sync_ref, converged_ref)
      |> Map.put(:sync_retry_ref, retry_ref)
    end)

    send(
      RaftBootstrap,
      {converged_ref, {:ok, :coordinator, true, %{probe_result: "ok", failing_group_id: nil}}}
    )

    eventually(fn ->
      assert %{sync_ref: nil, sync_retry_ref: nil} = :sys.get_state(RaftBootstrap)
    end)
  end

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
