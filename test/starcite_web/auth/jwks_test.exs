defmodule StarciteWeb.Auth.JWKSTest do
  use ExUnit.Case, async: false

  alias StarciteWeb.Auth.JWKS

  @cache_table :starcite_auth_jwks_cache

  test "clear_cache is idempotent" do
    assert :ok = JWKS.clear_cache()
    assert :ok = JWKS.clear_cache()
  end

  test "clear_cache removes cached entries when cache table exists" do
    table = ensure_cache_table()
    url = "http://jwks.example/#{System.unique_integer([:positive, :monotonic])}"
    expires_at_ms = System.system_time(:millisecond) + 1_000
    keys_by_kid = %{"kid-1" => "signer"}

    assert true = :ets.insert(table, {url, expires_at_ms, keys_by_kid})
    assert [{^url, ^expires_at_ms, ^keys_by_kid}] = :ets.lookup(table, url)

    assert :ok = JWKS.clear_cache()
    assert [] == :ets.lookup(table, url)
  end

  defp ensure_cache_table do
    case :ets.whereis(@cache_table) do
      :undefined ->
        :ets.new(@cache_table, [
          :named_table,
          :set,
          :public,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      table ->
        table
    end
  rescue
    ArgumentError ->
      @cache_table
  end
end
