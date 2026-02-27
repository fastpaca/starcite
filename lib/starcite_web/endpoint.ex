defmodule StarciteWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :starcite

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug Plug.Head
  plug StarciteWeb.Plugs.RedactSensitiveQuery
  # Expose Prometheus metrics at /metrics via PromEx
  plug PromEx.Plug,
    prom_ex_module: Starcite.Observability.PromEx,
    auth_strategy: :none

  plug StarciteWeb.Router
end
