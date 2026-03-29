defmodule Starcite.DataPlane.SessionRuntime do
  @moduledoc """
  Local lifecycle owner for one loaded session replica.

  `SessionRuntime` owns hydrate/freeze, idle tracking, lifecycle events, and
  the lifetime of the underlying `SessionLog` process.
  """

  use GenServer

  alias Starcite.DataPlane.SessionLog
  alias Starcite.DataPlane.SessionStore
  alias Starcite.Observability.Telemetry
  alias Starcite.Routing.SessionRouter
  alias Starcite.Session

  @registry Starcite.DataPlane.SessionRuntimeRegistry
  @call_timeout Application.compile_env(:starcite, :session_log_call_timeout_ms, 2_000)

  @type role :: SessionLog.role()
  @type data :: %{
          required(:session_id) => String.t(),
          required(:tenant_id) => String.t(),
          required(:log_pid) => pid(),
          required(:role) => role(),
          required(:idle_timeout_ms) => pos_integer() | :infinity,
          required(:idle_check_interval_ms) => pos_integer(),
          required(:last_activity_mono_ms) => integer(),
          required(:idle_timer_ref) => reference() | nil
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    session = Keyword.fetch!(opts, :session)

    GenServer.start_link(__MODULE__, opts, name: via(session.id))
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

  @spec log_pid(pid()) :: {:ok, pid()} | {:error, :session_runtime_unavailable}
  def log_pid(pid) when is_pid(pid) do
    call(pid, :log_pid)
  end

  @spec checkout(pid(), term()) :: {:ok, pid()} | {:error, :session_runtime_unavailable}
  def checkout(pid, message) when is_pid(pid) do
    call(pid, {:checkout, message})
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
      |> normalize_session_epoch()
      |> apply_routing_epoch()

    role = role_for_session(runtime_session.id)

    {:ok, log_pid} =
      SessionLog.start_link(
        session: runtime_session,
        role: role,
        replicate_fun: replicate_fun,
        name: SessionLog.via(runtime_session.id)
      )

    if start_reason == :hydrate do
      :ok = emit_lifecycle(runtime_session, role, "session.hydrating")

      :ok =
        Telemetry.session_hydrate(runtime_session.id, runtime_session.tenant_id, :ok, :hydrate)
    end

    :ok = emit_lifecycle(runtime_session, role, "session.activated")

    now = now_monotonic_ms()

    {:ok,
     %{
       session_id: runtime_session.id,
       tenant_id: runtime_session.tenant_id,
       log_pid: log_pid,
       role: role,
       idle_timeout_ms: idle_timeout_ms,
       idle_check_interval_ms: idle_check_interval_ms,
       last_activity_mono_ms: now,
       idle_timer_ref: nil
     }
     |> schedule_idle_tick()}
  end

  @impl true
  def handle_call(:log_pid, _from, %{log_pid: log_pid} = data) when is_pid(log_pid) do
    {:reply, {:ok, log_pid}, data}
  end

  def handle_call({:checkout, message}, _from, %{log_pid: log_pid} = data) when is_pid(log_pid) do
    next_data = if activity_message?(message), do: touch_activity(data), else: data
    {:reply, {:ok, log_pid}, next_data}
  end

  @impl true
  def handle_info(
        {:timeout, idle_timer_ref, :idle_tick},
        %{idle_timer_ref: idle_timer_ref} = data
      )
      when is_reference(idle_timer_ref) do
    data = %{data | idle_timer_ref: nil}

    if should_freeze?(data) do
      :ok = emit_lifecycle(data, "session.freezing")
      :ok = Telemetry.session_freeze(data.session_id, data.tenant_id, :ok, :idle_timeout)
      :ok = emit_lifecycle(data, "session.frozen")
      :ok = stop_log(data.log_pid)
      {:stop, :normal, data}
    else
      {:noreply, schedule_idle_tick(data)}
    end
  end

  def handle_info(_message, data), do: {:noreply, data}

  defp call(pid, message) when is_pid(pid) do
    GenServer.call(pid, message, @call_timeout)
  catch
    :exit, _reason -> {:error, :session_runtime_unavailable}
  end

  defp normalize_session_epoch(%Session{epoch: epoch} = session)
       when is_integer(epoch) and epoch >= 0 do
    session
  end

  defp normalize_session_epoch(%Session{} = session), do: %Session{session | epoch: 0}

  defp apply_routing_epoch(%Session{id: session_id} = session)
       when is_binary(session_id) and session_id != "" do
    normalized_session = normalize_session_epoch(session)
    fallback_epoch = normalize_epoch(normalized_session.epoch)
    routing_epoch = SessionRouter.local_owner_epoch(session_id, fallback_epoch)
    %Session{normalized_session | epoch: routing_epoch}
  end

  defp normalize_epoch(epoch) when is_integer(epoch) and epoch >= 0, do: epoch
  defp normalize_epoch(_epoch), do: 0

  defp role_for_session(session_id) when is_binary(session_id) and session_id != "" do
    case SessionRouter.ensure_local_owner(session_id) do
      :ok -> :owner
      _other -> :follower
    end
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
         role: :owner,
         session_id: session_id,
         log_pid: log_pid,
         idle_timeout_ms: idle_timeout_ms,
         last_activity_mono_ms: last_activity_mono_ms
       })
       when is_pid(log_pid) and is_binary(session_id) and session_id != "" do
    idle_timeout_ms != :infinity and
      idle_elapsed_ms(last_activity_mono_ms) >= idle_timeout_ms and
      SessionRouter.ensure_local_owner(session_id) == :ok and
      log_fully_archived?(log_pid)
  end

  defp should_freeze?(_data), do: false

  defp log_fully_archived?(log_pid) when is_pid(log_pid) do
    case :gen_batch_server.call(log_pid, :describe, @call_timeout) do
      {:ok, %{role: :owner, session: %Session{last_seq: last_seq, archived_seq: archived_seq}}} ->
        last_seq == archived_seq

      _other ->
        false
    end
  catch
    :exit, _reason -> false
  end

  defp emit_lifecycle(%Session{} = session, role, kind) when is_binary(kind) and kind != "" do
    emit_lifecycle(%{session_id: session.id, tenant_id: session.tenant_id, role: role}, kind)
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

  defp schedule_idle_tick(%{role: :owner, idle_timeout_ms: :infinity} = data) do
    cancel_idle_tick(data.idle_timer_ref)
    %{data | idle_timer_ref: nil}
  end

  defp schedule_idle_tick(%{role: :owner} = data) do
    cancel_idle_tick(data.idle_timer_ref)

    idle_timer_ref =
      :erlang.start_timer(
        min(data.idle_timeout_ms, data.idle_check_interval_ms),
        self(),
        :idle_tick
      )

    %{data | idle_timer_ref: idle_timer_ref}
  end

  defp schedule_idle_tick(data), do: %{data | idle_timer_ref: nil}

  defp cancel_idle_tick(idle_timer_ref) do
    if is_reference(idle_timer_ref) do
      _ = :erlang.cancel_timer(idle_timer_ref, [{:async, false}, {:info, false}])
    end

    :ok
  end

  defp stop_log(log_pid) when is_pid(log_pid) do
    _ = :gen_batch_server.stop(log_pid)
    :ok
  catch
    :exit, _reason -> :ok
  end
end
