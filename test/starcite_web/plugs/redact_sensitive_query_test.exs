defmodule StarciteWeb.Plugs.RedactSensitiveQueryTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias StarciteWeb.Plugs.RedactSensitiveQuery

  test "redacts access_token query param and preserves raw token in private assigns" do
    conn = conn(:get, "/v1/sessions/ses-1/tail?cursor=0&access_token=abc.def")

    redacted_conn = RedactSensitiveQuery.call(conn, %{})

    assert URI.decode_query(redacted_conn.query_string) == %{
             "cursor" => "0",
             "access_token" => "[REDACTED]"
           }

    assert redacted_conn.private[RedactSensitiveQuery.ws_access_token_private_key()] == "abc.def"
  end

  test "leaves query string unchanged when no access_token is present" do
    conn = conn(:get, "/v1/sessions/ses-1/tail?cursor=7")
    redacted_conn = RedactSensitiveQuery.call(conn, %{})

    assert redacted_conn.query_string == "cursor=7"
    refute Map.has_key?(redacted_conn.private, RedactSensitiveQuery.ws_access_token_private_key())
  end

  test "redacts access_token even when the token value has invalid encoding sequences" do
    conn = %{conn(:get, "/") | query_string: "access_token=%ZZ&cursor=0"}
    redacted_conn = RedactSensitiveQuery.call(conn, %{})

    assert URI.decode_query(redacted_conn.query_string) == %{
             "cursor" => "0",
             "access_token" => "[REDACTED]"
           }

    assert redacted_conn.private[RedactSensitiveQuery.ws_access_token_private_key()] == "%ZZ"
  end
end
