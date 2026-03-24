defmodule StarciteWeb.TailChannel do
  @moduledoc """
  Phoenix channel for a single tailed session topic.

  Clients join `tail:<session_id>` with a resume cursor and optional replay
  batch size. The channel replays committed events after that cursor, streams
  new commits as `events`, emits `gap` when continuity is unavailable, and
  terminates with `token_expired` when JWT lifetime is exhausted.
  """

  use StarciteWeb, :channel

  alias Starcite.ReadPath
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.Policy
  alias StarciteWeb.TailParams
  alias StarciteWeb.TailStream

  @impl true
  def join("tail:" <> session_id, params, %{assigns: %{auth: %Context{} = auth}} = socket)
      when is_binary(session_id) and session_id != "" and is_map(params) do
    with :ok <- ensure_auth_current(auth),
         {:ok, %{cursor: cursor, frame_batch_size: frame_batch_size}} <- TailParams.parse(params),
         :ok <- Policy.allowed_to_access_session(auth, session_id),
         {:ok, session} <- ReadPath.get_session_routed(session_id, true),
         :ok <- Policy.allowed_to_read_session(auth, session),
         {:ok, state} <-
           TailStream.init(%{
             session_id: session_id,
             cursor: cursor,
             frame_batch_size: frame_batch_size,
             principal: auth.principal,
             auth_context: auth
           }) do
      {:ok, assign(socket, :tail_state, state)}
    else
      {:error, reason} -> {:error, %{reason: to_string(reason)}}
    end
  end

  def join("tail:" <> _session_id, _params, _socket), do: {:error, %{reason: "unauthorized"}}
  def join(_topic, _params, _socket), do: {:error, %{reason: "invalid_session_id"}}

  @impl true
  def handle_in(_event, _payload, socket), do: {:noreply, socket}

  @impl true
  def handle_info(message, %{assigns: %{tail_state: state}} = socket) do
    case TailStream.handle_info(message, state) do
      {:ok, next_state} ->
        {:noreply, assign(socket, :tail_state, next_state)}

      {:emit, {:events, events}, next_state} ->
        :ok = push(socket, "events", %{events: events})
        {:noreply, assign(socket, :tail_state, next_state)}

      {:emit, {:gap, gap}, next_state} ->
        :ok = push(socket, "gap", gap)
        {:noreply, assign(socket, :tail_state, next_state)}

      {:close, _code, "token_expired" = reason, next_state} ->
        :ok = push(socket, "token_expired", %{reason: reason})
        {:stop, {:shutdown, :token_expired}, assign(socket, :tail_state, next_state)}

      {:stop, _reason, next_state} ->
        {:stop, :normal, assign(socket, :tail_state, next_state)}
    end
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  defp ensure_auth_current(%Context{expires_at: expires_at})
       when is_integer(expires_at) and expires_at > 0 do
    if expires_at <= System.system_time(:second), do: {:error, :token_expired}, else: :ok
  end

  defp ensure_auth_current(%Context{}), do: :ok
end
