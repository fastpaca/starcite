defmodule Starcite.DataPlane.SessionRuntime do
  @moduledoc """
  Local lifecycle owner and batching server for one loaded session replica.

  `SessionRuntime` owns hydrate/freeze, idle tracking, lifecycle events, and
  the batched mailbox around the in-memory `SessionLog` state machine.
  """

  @behaviour :gen_batch_server

  alias Starcite.DataPlane.SessionLog
  alias Starcite.DataPlane.SessionStore
  alias Starcite.Observability.Telemetry
  alias Starcite.Routing.SessionRouter
  alias Starcite.Session

  @registry Starcite.DataPlane.SessionRuntimeRegistry
  @min_batch_size Application.compile_env(:starcite, :session_log_batch_min_size, 8)
  @max_batch_size Application.compile_env(:starcite, :session_log_batch_max_size, 256)

  @type role :: SessionLog.role()
  @type data :: %{
          required(:log) => SessionLog.t(),
          required(:idle_timeout_ms) => pos_integer() | :infinity,
          required(:idle_check_interval_ms) => pos_integer(),
          required(:last_activity_mono_ms) => integer(),
          required(:idle_timer_ref) => reference() | nil
        }

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) when is_list(opts) do
    session = Keyword.fetch!(opts, :session)

    :gen_batch_server.start_link(
      via(session.id),
      __MODULE__,
      opts,
      min_batch_size: @min_batch_size,
      max_batch_size: @max_batch_size
    )
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    session = Keyword.fetch!(opts, :session)

    %{
      id: {__MODULE__, session.id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      shutdown: 500,
      type: :worker
    }
  end

  @spec via(String.t()) :: {:via, Registry, {module(), String.t()}}
  def via(session_id) when is_binary(session_id) and session_id != "" do
    {:via, Registry, {@registry, session_id}}
  end

  @impl true
  def init(opts) do
    Process.flag(:message_queue_data, :off_heap)

    session = Keyword.fetch!(opts, :session)
    start_reason = Keyword.get(opts, :start_reason, :startup)
    replicate_fun = Keyword.fetch!(opts, :replicate_fun)
    idle_timeout_ms = Keyword.fetch!(opts, :idle_timeout_ms)
    idle_check_interval_ms = Keyword.fetch!(opts, :idle_check_interval_ms)

    resolved_session =
      case SessionStore.get_session_cached(session.id) do
        {:ok, %Session{} = loaded} -> loaded
        :error -> session
      end

    runtime_session =
      resolved_session
      |> Session.normalize_epoch()
      |> apply_routing_epoch()

    role = role_for_session(runtime_session.id)

    if start_reason == :hydrate do
      :ok = emit_lifecycle(runtime_session, role, "session.hydrating")

      :ok =
        Telemetry.session_hydrate(runtime_session.id, runtime_session.tenant_id, :ok, :hydrate)
    end

    :ok = emit_lifecycle(runtime_session, role, "session.activated")

    {:ok,
     %{
       log: SessionLog.new(runtime_session, role, replicate_fun),
       idle_timeout_ms: idle_timeout_ms,
       idle_check_interval_ms: idle_check_interval_ms,
       last_activity_mono_ms: now_monotonic_ms(),
       idle_timer_ref: nil
     }
     |> schedule_idle_tick()}
  end

  @impl true
  def handle_batch(batch, data) when is_list(batch) do
    data = if activity_batch?(batch), do: touch_activity(data), else: data

    case process_runtime_batch(batch, data, []) do
      {:stop, reason} ->
        {:stop, reason}

      {next_data, actions} ->
        {:ok, actions, next_data}
    end
  end

  defp process_runtime_batch([], data, actions), do: {data, actions}

  defp process_runtime_batch(
         [{:info, {:timeout, idle_timer_ref, :idle_tick}} | rest],
         %{idle_timer_ref: idle_timer_ref} = data,
         actions
       )
       when is_reference(idle_timer_ref) do
    data = %{data | idle_timer_ref: nil}

    if should_freeze?(data) do
      :ok = emit_lifecycle(data, "session.freezing")
      :ok = Telemetry.session_freeze(session_id(data), tenant_id(data), :ok, :idle_timeout)
      :ok = emit_lifecycle(data, "session.frozen")
      {:stop, :normal}
    else
      process_runtime_batch(rest, schedule_idle_tick(data), actions)
    end
  end

  defp process_runtime_batch(batch, %{log: log} = data, actions) do
    {log_batch, rest} = collect_log_batch(batch, data.idle_timer_ref, [])
    {next_log, log_actions} = SessionLog.handle_batch(log_batch, log)
    next_data = reconcile_idle_tick(data, %{data | log: next_log})
    process_runtime_batch(rest, next_data, actions ++ log_actions)
  end

  defp collect_log_batch(
         [{:info, {:timeout, idle_timer_ref, :idle_tick}} | _rest] = batch,
         idle_timer_ref,
         acc
       )
       when is_reference(idle_timer_ref) do
    {Enum.reverse(acc), batch}
  end

  defp collect_log_batch([message | rest], idle_timer_ref, acc) do
    collect_log_batch(rest, idle_timer_ref, [message | acc])
  end

  defp collect_log_batch([], _idle_timer_ref, acc), do: {Enum.reverse(acc), []}

  defp reconcile_idle_tick(%{log: previous_log}, %{log: next_log} = data) do
    case {SessionLog.role(previous_log), SessionLog.role(next_log)} do
      {same_role, same_role} ->
        data

      {_old_role, :owner} ->
        schedule_idle_tick(data)

      {_old_role, _new_role} ->
        cancel_idle_tick(data.idle_timer_ref)
        %{data | idle_timer_ref: nil}
    end
  end

  defp apply_routing_epoch(%Session{id: session_id} = session)
       when is_binary(session_id) and session_id != "" do
    normalized_session = Session.normalize_epoch(session)
    fallback_epoch = Session.normalize_epoch_value(normalized_session.epoch)
    routing_epoch = SessionRouter.local_owner_epoch(session_id, fallback_epoch)
    %Session{normalized_session | epoch: routing_epoch}
  end

  defp role_for_session(session_id) when is_binary(session_id) and session_id != "" do
    case SessionRouter.ensure_local_owner(session_id) do
      :ok -> :owner
      _other -> :follower
    end
  end

  defp activity_batch?(batch) when is_list(batch) do
    Enum.any?(batch, fn
      {:call, _from, message} -> activity_message?(message)
      _other -> false
    end)
  end

  defp touch_activity(%{last_activity_mono_ms: _last} = data) do
    %{data | last_activity_mono_ms: now_monotonic_ms()}
  end

  defp activity_message?(:get_session), do: true
  defp activity_message?(:fetch_cursor_snapshot), do: true
  defp activity_message?({:append_event, _input, _expected_seq, _replicas}), do: true
  defp activity_message?({:append_events, _inputs, _expected_seq, _replicas}), do: true
  defp activity_message?(_message), do: false

  defp should_freeze?(%{
         log: log,
         idle_timeout_ms: idle_timeout_ms,
         last_activity_mono_ms: last_activity_mono_ms
       })
       when idle_timeout_ms != :infinity do
    role = SessionLog.role(log)
    session = SessionLog.session(log)

    role == :owner and
      idle_elapsed_ms(last_activity_mono_ms) >= idle_timeout_ms and
      SessionRouter.ensure_local_owner(session.id) == :ok and
      session.last_seq == session.archived_seq
  end

  defp should_freeze?(_data), do: false

  defp emit_lifecycle(%Session{} = session, role, kind) when is_binary(kind) and kind != "" do
    emit_lifecycle(%{session_id: session.id, tenant_id: session.tenant_id, role: role}, kind)
  end

  defp emit_lifecycle(%{log: log}, kind) when is_binary(kind) and kind != "" do
    emit_lifecycle(SessionLog.session(log), SessionLog.role(log), kind)
  end

  defp emit_lifecycle(%{role: :owner, session_id: session_id, tenant_id: tenant_id}, kind)
       when is_binary(kind) and kind != "" do
    Phoenix.PubSub.broadcast(
      Starcite.PubSub,
      "lifecycle:" <> tenant_id,
      {:session_lifecycle,
       %{
         kind: kind,
         session_id: session_id,
         tenant_id: tenant_id
       }}
    )
  end

  defp emit_lifecycle(_data, _kind), do: :ok

  defp now_monotonic_ms, do: System.monotonic_time(:millisecond)

  defp idle_elapsed_ms(last_activity_mono_ms)
       when is_integer(last_activity_mono_ms) do
    max(now_monotonic_ms() - last_activity_mono_ms, 0)
  end

  defp schedule_idle_tick(%{log: log, idle_timeout_ms: :infinity} = data) do
    if SessionLog.role(log) == :owner do
      cancel_idle_tick(data.idle_timer_ref)
      %{data | idle_timer_ref: nil}
    else
      %{data | idle_timer_ref: nil}
    end
  end

  defp schedule_idle_tick(%{log: log} = data) do
    if SessionLog.role(log) == :owner do
      cancel_idle_tick(data.idle_timer_ref)

      idle_timer_ref =
        :erlang.start_timer(
          min(data.idle_timeout_ms, data.idle_check_interval_ms),
          self(),
          :idle_tick
        )

      %{data | idle_timer_ref: idle_timer_ref}
    else
      %{data | idle_timer_ref: nil}
    end
  end

  defp cancel_idle_tick(idle_timer_ref) do
    if is_reference(idle_timer_ref) do
      _ = :erlang.cancel_timer(idle_timer_ref, [{:async, false}, {:info, false}])
    end

    :ok
  end

  defp session_id(%{log: log}), do: SessionLog.session(log).id
  defp tenant_id(%{log: log}), do: SessionLog.session(log).tenant_id
end
