defmodule StarciteWeb.UserSocketTest do
  use ExUnit.Case, async: false
  import Phoenix.ChannelTest

  alias Starcite.AuthTestSupport
  alias StarciteWeb.Auth.JWKS
  alias StarciteWeb.UserSocket

  @auth_env_key StarciteWeb.Auth
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"
  @endpoint StarciteWeb.Endpoint

  setup do
    previous_auth = Application.get_env(:starcite, @auth_env_key)
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-#{System.unique_integer([:positive, :monotonic])}"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.stub(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.resp(200, Jason.encode!(jwks))
    end)

    Application.put_env(
      :starcite,
      @auth_env_key,
      issuer: @issuer,
      audience: @audience,
      jwks_url: "http://localhost:#{bypass.port}#{@jwks_path}",
      jwt_leeway_seconds: 0,
      jwks_refresh_ms: 1_000
    )

    on_exit(fn ->
      if is_nil(previous_auth) do
        Application.delete_env(:starcite, @auth_env_key)
      else
        Application.put_env(:starcite, @auth_env_key, previous_auth)
      end

      :ok = JWKS.clear_cache()
    end)

    {:ok, private_key: private_key, kid: kid}
  end

  test "connect authenticates a bearer token and assigns socket auth", %{
    private_key: private_key,
    kid: kid
  } do
    token =
      token_for(
        private_key,
        kid,
        %{"sub" => "user:user-42", "scopes" => ["session:read"], "tenant_id" => "acme"}
      )

    assert {:ok, socket} =
             UserSocket.connect(
               %{"token" => token},
               socket(UserSocket, "client-1", %{}),
               %{}
             )

    assert socket.assigns.auth.principal.tenant_id == "acme"
    assert socket.assigns.auth.scopes == ["session:read"]
    assert is_binary(socket.assigns.disconnect_topic)
    assert UserSocket.id(socket) == socket.assigns.disconnect_topic
  end

  test "connect rejects missing tokens in JWT mode" do
    assert :error = UserSocket.connect(%{}, socket(UserSocket, "client-2", %{}), %{})
  end

  defp token_for(private_key, kid, claims) do
    now = System.system_time(:second)

    claims =
      claims
      |> Map.put_new("iss", @issuer)
      |> Map.put_new("aud", @audience)
      |> Map.put_new("iat", now)
      |> Map.put_new("nbf", now)
      |> Map.put_new("exp", now + 300)

    AuthTestSupport.sign_rs256(private_key, claims, kid)
  end
end
