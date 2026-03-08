defmodule StarciteWeb.OpsRouter do
  @moduledoc false

  use Plug.Router

  import Plug.Conn

  alias StarciteWeb.Health

  plug :match
  plug :dispatch

  get "/health/live" do
    send_json(conn, Health.live())
  end

  get "/health/ready" do
    send_json(conn, Health.ready())
  end

  forward "/metrics",
    to: PromEx.Plug,
    init_opts: [prom_ex_module: Starcite.Observability.PromEx, auth_strategy: :none]

  forward "/",
    to: Starcite.Pprof.Router

  defp send_json(conn, {status, body}) when is_integer(status) and is_map(body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(body))
  end
end
