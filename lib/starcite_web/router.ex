defmodule StarciteWeb.Router do
  use StarciteWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  pipeline :service_auth do
    # Service credentials are mandatory for privileged issuance paths.
    plug StarciteWeb.Plugs.ServiceAuth, required: true
  end

  pipeline :service_or_token_auth do
    # Order is security-sensitive:
    # 1) try service auth first
    # 2) only if service auth fails, allow principal token auth
    # This keeps service and principal trust domains separate.
    plug StarciteWeb.Plugs.ServiceAuth, required: false
    plug StarciteWeb.Plugs.PrincipalAuth
  end

  scope "/v1", StarciteWeb do
    pipe_through [:api, :service_auth]

    post "/auth/issue", AuthTokenController, :issue
  end

  scope "/v1/ops", StarciteWeb do
    pipe_through [:api, :service_auth]

    get "/status", OpsController, :status
    get "/ready-nodes", OpsController, :ready_nodes
    post "/drain", OpsController, :drain
    post "/undrain", OpsController, :undrain
    get "/groups/:group_id/replicas", OpsController, :group_replicas
  end

  scope "/v1", StarciteWeb do
    pipe_through [:api, :service_or_token_auth]

    post "/sessions", SessionController, :create
    get "/sessions", SessionController, :index
    post "/sessions/:id/append", SessionController, :append
    get "/sessions/:id/tail", TailController, :tail
  end

  scope "/", StarciteWeb do
    get "/health/live", HealthController, :live
    get "/health/ready", HealthController, :ready
  end
end
