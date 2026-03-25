defmodule StarciteWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :starcite

  socket "/v1/socket", StarciteWeb.UserSocket,
    websocket: true,
    longpoll: false

  plug StarciteWeb.Plugs.EdgeStage, :start
  plug Plug.Telemetry, event_prefix: [:phoenix, :endpoint], log: false

  plug CORSPlug,
    origin: "*",
    credentials: false,
    headers: ["*"],
    expose: ["*"]

  plug Plug.Head
  plug StarciteWeb.Plugs.ServiceAuth

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug StarciteWeb.Plugs.EdgeStage, :controller_entry
  plug StarciteWeb.Router
end
