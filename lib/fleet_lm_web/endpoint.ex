defmodule FleetLMWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :fleet_lm

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug Plug.Head
  # Expose Prometheus metrics at /metrics via PromEx
  plug PromEx.Plug,
    prom_ex_module: FleetLM.Observability.PromEx,
    auth_strategy: :none

  plug FleetLMWeb.Router
end
