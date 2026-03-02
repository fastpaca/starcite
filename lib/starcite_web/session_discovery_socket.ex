defmodule StarciteWeb.SessionDiscoverySocket do
  @moduledoc """
  Raw WebSocket handler for service-level session lifecycle discovery.

  Emits JSON text frames for `session_created`, `session_frozen`, and
  `session_hydrated` updates.
  """

  @behaviour WebSock

  alias Phoenix.PubSub
  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.SessionDiscovery
  alias StarciteWeb.Auth.Context

  @impl true
  def init(%{auth_context: %Context{} = auth_context}) do
    :ok = PubSub.subscribe(Starcite.PubSub, SessionDiscovery.topic())
    auth_expires_at = auth_expires_at(auth_context)

    {:ok,
     %{
       auth_context: auth_context,
       auth_expires_at: auth_expires_at,
       auth_expiry_timer_ref: nil
     }
     |> schedule_auth_expiry()}
  end

  @impl true
  def handle_in({_payload, opcode: _opcode}, state) do
    # Session discovery socket is server->client only.
    {:ok, state}
  end

  @impl true
  def handle_info(:auth_expired, state),
    do: {:stop, :token_expired, {4001, "token_expired"}, state}

  def handle_info({:session_discovery, update}, state) when is_map(update) do
    if allow_update?(state.auth_context, update) do
      payload = update |> encode_payload() |> Jason.encode!()
      {:push, {:text, payload}, state}
    else
      {:ok, state}
    end
  end

  def handle_info(_message, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _state), do: :ok

  defp allow_update?(%Context{kind: :none}, _update), do: true

  defp allow_update?(
         %Context{kind: :jwt, principal: %Principal{tenant_id: tenant_id}, session_id: nil},
         update
       )
       when is_binary(tenant_id) and tenant_id != "" do
    update_tenant_id(update) == tenant_id
  end

  defp allow_update?(
         %Context{
           kind: :jwt,
           principal: %Principal{tenant_id: tenant_id},
           session_id: allowed_session_id
         },
         update
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(allowed_session_id) and
              allowed_session_id != "" do
    update_tenant_id(update) == tenant_id and update_session_id(update) == allowed_session_id
  end

  defp allow_update?(_auth, _update), do: false

  defp update_tenant_id(update), do: map_get_binary(update, :tenant_id)
  defp update_session_id(update), do: map_get_binary(update, :session_id)

  defp map_get_binary(map, key) when is_map(map) and is_atom(key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" ->
        value

      _ ->
        case Map.get(map, Atom.to_string(key)) do
          value when is_binary(value) and value != "" -> value
          _ -> nil
        end
    end
  end

  defp encode_payload(payload) when is_map(payload) do
    payload
    |> Enum.into(%{}, fn {key, value} ->
      {to_string(key), encode_payload_value(value)}
    end)
  end

  defp encode_payload_value(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp encode_payload_value(%NaiveDateTime{} = value) do
    value
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp encode_payload_value(value) when is_atom(value), do: Atom.to_string(value)

  defp encode_payload_value(value) when is_map(value), do: encode_payload(value)

  defp encode_payload_value(value) when is_list(value),
    do: Enum.map(value, &encode_payload_value/1)

  defp encode_payload_value(value), do: value

  defp schedule_auth_expiry(%{auth_expires_at: expires_at} = state)
       when is_integer(expires_at) and expires_at > 0 do
    now_ms = System.system_time(:millisecond)
    expires_at_ms = expires_at * 1000

    if expires_at_ms <= now_ms do
      send(self(), :auth_expired)
      state
    else
      ref = Process.send_after(self(), :auth_expired, expires_at_ms - now_ms)
      %{state | auth_expiry_timer_ref: ref}
    end
  end

  defp schedule_auth_expiry(state), do: state

  defp auth_expires_at(%Context{expires_at: expires_at})
       when is_integer(expires_at) and expires_at > 0,
       do: expires_at

  defp auth_expires_at(%Context{}), do: nil
end
