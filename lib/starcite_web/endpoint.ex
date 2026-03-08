defmodule StarciteWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :starcite

  plug CORSPlug,
    origin: "*",
    credentials: false,
    headers: ["*"],
    expose: ["*"]

  plug Plug.Head
  plug StarciteWeb.Plugs.RedactSensitiveQuery
  plug StarciteWeb.Plugs.ServiceAuth

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug StarciteWeb.Router
end
