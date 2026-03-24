defmodule StarciteWeb.UserSocket do
  use Phoenix.Socket

  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Plugs.ServiceAuth

  channel "tail:*", StarciteWeb.TailChannel

  @impl true
  def connect(params, socket, _connect_info) when is_map(params) do
    with {:ok, auth} <- authenticate(params) do
      disconnect_topic = "socket:#{random_id()}"

      {:ok,
       socket
       |> assign(:auth, auth)
       |> assign(:disconnect_topic, disconnect_topic)}
    else
      {:error, _reason} -> :error
    end
  end

  def connect(_params, _socket, _connect_info), do: :error

  @impl true
  def id(%Phoenix.Socket{assigns: %{disconnect_topic: disconnect_topic}}), do: disconnect_topic
  def id(_socket), do: nil

  defp authenticate(%{"token" => token, "access_token" => access_token})
       when is_binary(token) and is_binary(access_token) do
    {:error, :invalid_bearer_token}
  end

  defp authenticate(%{"token" => token}) when is_binary(token) and token != "" do
    ServiceAuth.authenticate_token(token)
  end

  defp authenticate(%{"access_token" => token}) when is_binary(token) and token != "" do
    ServiceAuth.authenticate_token(token)
  end

  defp authenticate(_params) do
    case ServiceAuth.mode() do
      :none -> {:ok, Context.none()}
      :jwt -> {:error, :missing_bearer_token}
    end
  end

  defp random_id do
    Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
  end
end
