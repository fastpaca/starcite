defmodule StarciteWeb.LifecycleChannel do
  @moduledoc """
  Phoenix channel for live tenant-scoped session lifecycle notifications.

  Clients join the single `lifecycle` topic. The server infers tenant scope from
  socket auth and pushes ephemeral lifecycle events as they are published on
  tenant PubSub.
  """

  use StarciteWeb, :channel

  alias Phoenix.PubSub
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.Policy

  @impl true
  def join("lifecycle", _params, %{assigns: %{auth: %Context{} = auth}} = socket) do
    with :ok <- Context.ensure_current(auth),
         {:ok, tenant_id} <- Policy.authorize_lifecycle_subscription(auth),
         :ok <- PubSub.subscribe(Starcite.PubSub, topic(tenant_id)) do
      {:ok, schedule_auth_expiry(socket, auth)}
    else
      {:error, reason} -> {:error, %{reason: to_string(reason)}}
    end
  end

  def join(_topic, _params, _socket), do: {:error, %{reason: "unauthorized"}}

  @impl true
  def handle_in(_event, _payload, socket), do: {:noreply, socket}

  @impl true
  def handle_info({:session_lifecycle, event}, socket) when is_map(event) do
    :ok = push(socket, "lifecycle", %{event: event})
    {:noreply, socket}
  end

  def handle_info(:auth_expired, socket) do
    :ok = push(socket, "token_expired", %{reason: "token_expired"})
    {:stop, {:shutdown, :token_expired}, socket}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  defp schedule_auth_expiry(socket, %Context{expires_at: expires_at})
       when is_integer(expires_at) and expires_at > 0 do
    delay_ms = max(expires_at - System.system_time(:second), 0) * 1_000
    Process.send_after(self(), :auth_expired, delay_ms)
    socket
  end

  defp schedule_auth_expiry(socket, %Context{}), do: socket

  defp topic(tenant_id) when is_binary(tenant_id) and tenant_id != "" do
    "lifecycle:" <> tenant_id
  end
end
