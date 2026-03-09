defmodule StarciteWeb.TailUserSocket do
  @moduledoc false

  use Phoenix.Socket

  alias Phoenix.Socket
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Plugs.ServiceAuth

  defguardp present_token(token) when is_binary(token) and token != ""

  channel "tail:*", StarciteWeb.TailChannel

  @impl true
  def connect(params, %Socket{} = socket, connect_info)
      when is_map(params) and is_map(connect_info) do
    case authenticate(params, connect_info) do
      {:ok, %Context{} = auth_context} ->
        {:ok,
         socket
         |> assign(:auth_context, auth_context)
         |> assign(:socket_id, socket_id())}

      {:error, _reason} ->
        :error
    end
  end

  def connect(_params, _socket, _connect_info), do: :error

  @impl true
  def id(%Socket{assigns: %{socket_id: socket_id}}) when is_binary(socket_id), do: socket_id
  def id(_socket), do: nil

  defp authenticate(params, connect_info) do
    case ServiceAuth.mode() do
      :none ->
        {:ok, Context.none()}

      :jwt ->
        with {:ok, token} <- connect_token(params, connect_info),
             {:ok, auth_context} <- ServiceAuth.authenticate_token(token) do
          {:ok, auth_context}
        end
    end
  end

  defp connect_token(%{"access_token" => token}, %{auth_token: token}) when present_token(token),
    do: {:ok, token}

  defp connect_token(%{"access_token" => _query_token}, %{auth_token: _header_token}),
    do: {:error, :invalid_bearer_token}

  defp connect_token(_params, %{auth_token: token}) when present_token(token), do: {:ok, token}

  defp connect_token(%{"access_token" => token}, _connect_info) when present_token(token),
    do: {:ok, token}

  defp connect_token(_params, %{auth_token: _token}), do: {:error, :invalid_bearer_token}

  defp connect_token(%{"access_token" => _token}, _connect_info),
    do: {:error, :invalid_bearer_token}

  defp connect_token(_params, _connect_info), do: {:error, :missing_bearer_token}

  defp socket_id do
    "tail_socket:" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
  end
end
