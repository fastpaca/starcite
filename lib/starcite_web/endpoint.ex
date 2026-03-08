defmodule StarciteWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :starcite

  socket "/v1/tail/socket", StarciteWeb.TailUserSocket,
    websocket: [auth_token: true],
    longpoll: false

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
