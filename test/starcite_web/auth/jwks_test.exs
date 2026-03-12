defmodule StarciteWeb.Auth.JWKSTest do
  use ExUnit.Case, async: false

  alias Starcite.AuthTestSupport
  alias StarciteWeb.Auth.JWKS

  import Plug.Conn

  @cache_prefix :starcite_auth_jwks
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    :ok = JWKS.clear_cache()

    on_exit(fn ->
      :ok = JWKS.clear_cache()
    end)

    :ok
  end

  test "clear_cache is idempotent" do
    assert :ok = JWKS.clear_cache()
    assert :ok = JWKS.clear_cache()
  end

  test "clear_cache removes cached signer and metadata entries" do
    url = "http://jwks.example/#{System.unique_integer([:positive, :monotonic])}"
    signing_key = Joken.Signer.create("HS256", "secret")

    :persistent_term.put({@cache_prefix, :signing_key, url, "kid-1"}, signing_key)

    :persistent_term.put({@cache_prefix, :jwks_meta, url}, %{
      refresh_at_ms: System.system_time(:millisecond) + 1_000,
      expire_at_ms: System.system_time(:millisecond) + 5_000,
      kids: ["kid-1"]
    })

    assert is_struct(
             :persistent_term.get({@cache_prefix, :signing_key, url, "kid-1"}),
             Joken.Signer
           )

    assert %{} = :persistent_term.get({@cache_prefix, :jwks_meta, url})

    assert :ok = JWKS.clear_cache()
    assert nil == :persistent_term.get({@cache_prefix, :signing_key, url, "kid-1"}, nil)
    assert nil == :persistent_term.get({@cache_prefix, :jwks_meta, url}, nil)
  end

  test "fetch_signing_key refreshes once and serves cached signers by kid" do
    attach_auth_handler()
    {config, _private_key, kid, _bypass} = jwks_fixture!()

    assert {:ok, signer} = JWKS.fetch_signing_key(config, kid)
    assert is_struct(signer, Joken.Signer)
    assert_receive_auth_event(:jwks_fetch, :jwt, :ok, :none, :refresh)

    assert {:ok, cached_signer} = JWKS.fetch_signing_key(config, kid)
    assert is_struct(cached_signer, Joken.Signer)
    assert_receive_auth_event(:jwks_fetch, :jwt, :ok, :none, :cache)
  end

  test "stale cached signer returns cache immediately and refreshes asynchronously" do
    attach_auth_handler()
    test_pid = self()
    {config, _private_key, kid, bypass} = jwks_fixture!(jwks_refresh_ms: 10)

    assert {:ok, signer} = JWKS.fetch_signing_key(config, kid)
    assert is_struct(signer, Joken.Signer)
    assert_receive_auth_event(:jwks_fetch, :jwt, :ok, :none, :refresh)

    Process.sleep(20)

    Bypass.expect_once(bypass, "GET", @jwks_path, fn conn ->
      send(test_pid, :async_refresh)

      conn
      |> put_resp_content_type("application/json")
      |> resp(
        200,
        Jason.encode!(
          AuthTestSupport.jwks_for_private_key(AuthTestSupport.generate_rsa_private_key(), kid)
        )
      )
    end)

    assert {:ok, cached_signer} = JWKS.fetch_signing_key(config, kid)
    assert is_struct(cached_signer, Joken.Signer)
    assert_receive_auth_event(:jwks_fetch, :jwt, :ok, :none, :cache)
    assert_receive :async_refresh, 1_000
  end

  test "concurrent fetch_signing_key calls share one jwks refresh" do
    {config, _private_key, kid, _bypass} = jwks_fixture!()

    results =
      1..20
      |> Task.async_stream(
        fn _ -> JWKS.fetch_signing_key(config, kid) end,
        ordered: false,
        timeout: 5_000,
        max_concurrency: 20
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, fn result -> match?({:ok, %Joken.Signer{}}, result) end)
  end

  defp jwks_fixture!(overrides \\ []) do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-#{System.unique_integer([:positive, :monotonic])}"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect_once(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    config = %{
      jwks_url: "http://localhost:#{bypass.port}#{@jwks_path}",
      jwks_refresh_ms: Keyword.get(overrides, :jwks_refresh_ms, 60_000),
      issuer: @issuer,
      audience: @audience,
      mode: :jwt,
      jwt_leeway_seconds: 0
    }

    {config, private_key, kid, bypass}
  end

  defp attach_auth_handler do
    handler_id = "jwks-auth-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :auth],
        fn _event, measurements, metadata, pid ->
          send(pid, {:auth_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp assert_receive_auth_event(stage, mode, outcome, error_reason, source) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_auth_event(stage, mode, outcome, error_reason, source, deadline)
  end

  defp do_assert_receive_auth_event(stage, mode, outcome, error_reason, source, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:auth_event, %{count: 1, duration_ms: duration_ms},
       %{
         stage: ^stage,
         mode: ^mode,
         outcome: ^outcome,
         error_reason: ^error_reason,
         source: ^source
       }} ->
        assert is_integer(duration_ms)
        assert duration_ms >= 0
        :ok

      {:auth_event, _measurements, _metadata} ->
        do_assert_receive_auth_event(stage, mode, outcome, error_reason, source, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for auth telemetry stage=#{inspect(stage)} mode=#{inspect(mode)} outcome=#{inspect(outcome)} error_reason=#{inspect(error_reason)} source=#{inspect(source)}"
        )
    end
  end
end
