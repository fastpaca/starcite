defmodule Starcite.WritePath.Append do
  @moduledoc """
  Append orchestration for the write path.

  This module routes append commands to the correct Raft group and handles the
  hydrate-on-miss retry flow when a cold session has already been evicted from
  the leader's in-memory state.
  """

  alias Starcite.DataPlane.{RaftAccess, SessionStore}
  alias Starcite.Observability.Telemetry
  alias Starcite.WritePath
  alias Starcite.WritePath.CommandRouter

  @type append_result ::
          {:ok, %{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}}
          | {:error, term()}
          | {:timeout, term()}

  @type append_many_result ::
          {:ok,
           %{
             results: [%{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}],
             last_seq: non_neg_integer()
           }}
          | {:error, term()}
          | {:timeout, term()}

  @doc """
  Append one event to a session, hydrating from archive on session miss.
  """
  @spec append_event(String.t(), map()) :: append_result()
  def append_event(id, event) when is_binary(id) and id != "" and is_map(event) do
    command = {:append_event, id, event, nil}
    dispatch(id, command, :append_event_local, [id, event])
  end

  def append_event(_id, _event), do: {:error, :invalid_event}

  @doc """
  Append one event with write-path options such as `:expected_seq`.
  """
  @spec append_event(String.t(), map(), keyword()) :: append_result()
  def append_event(id, event, opts)
      when is_binary(id) and id != "" and is_map(event) and is_list(opts) do
    command = {:append_event, id, event, Keyword.get(opts, :expected_seq)}
    dispatch(id, command, :append_event_local, [id, event, opts])
  end

  def append_event(_id, _event, _opts), do: {:error, :invalid_event}

  @doc false
  def append_event_local(id, event)
      when is_binary(id) and id != "" and is_map(event) do
    append_local(id, {:append_event, id, event, nil})
  end

  def append_event_local(_id, _event), do: {:error, :invalid_event}

  @doc false
  def append_event_local(id, event, opts)
      when is_binary(id) and id != "" and is_map(event) and is_list(opts) do
    append_local(id, {:append_event, id, event, Keyword.get(opts, :expected_seq)})
  end

  def append_event_local(_id, _event, _opts), do: {:error, :invalid_event}

  @doc """
  Append a batch of events to a session, hydrating from archive on session miss.
  """
  @spec append_events(String.t(), [map()], keyword()) :: append_many_result()
  def append_events(id, events, opts \\ [])

  def append_events(id, events, opts)
      when is_binary(id) and id != "" and is_list(events) and events != [] and is_list(opts) do
    dispatch(id, {:append_events, id, events, opts}, :append_events_local, [id, events, opts])
  end

  def append_events(_id, _events, _opts), do: {:error, :invalid_event}

  @doc false
  def append_events_local(id, events, opts \\ [])

  def append_events_local(id, events, opts)
      when is_binary(id) and id != "" and is_list(events) and events != [] and is_list(opts) do
    append_local(id, {:append_events, id, events, opts})
  end

  def append_events_local(_id, _events, _opts), do: {:error, :invalid_event}

  defp dispatch(id, command, remote_fun, remote_args)
       when is_binary(id) and id != "" and is_atom(remote_fun) and is_list(remote_args) do
    CommandRouter.dispatch_session(
      id,
      &append_with_rehydrate(&1, id, command),
      WritePath,
      remote_fun,
      remote_args
    )
  end

  defp append_local(id, command) when is_binary(id) and id != "" do
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(id) do
      append_with_rehydrate(server_id, id, command)
    end
  end

  defp append_with_rehydrate(server_id, session_id, command)
       when is_atom(server_id) and is_binary(session_id) and session_id != "" do
    case CommandRouter.dispatch_server(server_id, command) do
      {:error, :session_not_found} -> hydrate_and_retry(server_id, session_id, command)
      result -> result
    end
  end

  defp hydrate_and_retry(server_id, session_id, command)
       when is_atom(server_id) and is_binary(session_id) and session_id != "" do
    case SessionStore.get_archived_session(session_id) do
      {:ok, hydrated_session} ->
        hydrate_retry(server_id, session_id, hydrated_session, command)

      {:error, reason} ->
        :ok = Telemetry.session_hydrate(session_id, "unknown", :error, hydrate_reason(reason))
        archive_read_error(reason)
    end
  end

  defp hydrate_retry(server_id, session_id, hydrated_session, command)
       when is_atom(server_id) and is_binary(session_id) and session_id != "" do
    tenant_id = hydrated_session.tenant_id

    with {:ok, hydrate_result} <-
           CommandRouter.dispatch_server(server_id, {:hydrate_session, hydrated_session}),
         true <- hydrate_result in [:hydrated, :already_hot],
         {:ok, _reply} = append_reply <- CommandRouter.dispatch_server(server_id, command) do
      :ok = Telemetry.session_hydrate(session_id, tenant_id, :ok, hydrate_result)
      append_reply
    else
      false ->
        :ok =
          Telemetry.session_hydrate(session_id, tenant_id, :error, :unexpected_hydrate_result)

        {:error, :archive_read_unavailable}

      {:error, reason} ->
        :ok = Telemetry.session_hydrate(session_id, tenant_id, :error, hydrate_reason(reason))
        archive_read_error(reason)

      {:timeout, _reason} = timeout ->
        :ok = Telemetry.session_hydrate(session_id, tenant_id, :error, :timeout)
        timeout
    end
  end

  defp archive_read_error(:invalid_snapshot), do: {:error, :archive_read_unavailable}
  defp archive_read_error(reason), do: {:error, reason}

  defp hydrate_reason(reason) when is_atom(reason), do: reason
  defp hydrate_reason(_reason), do: :unknown
end
