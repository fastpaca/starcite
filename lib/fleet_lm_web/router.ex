defmodule FleetLMWeb.Router do
  use FleetLMWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/v1", FleetLMWeb do
    pipe_through :api

    post "/sessions", SessionController, :create
    post "/sessions/:id/append", SessionController, :append
    get "/sessions/:id/tail", TailController, :tail
  end

  scope "/", FleetLMWeb do
    get "/health/live", HealthController, :live
    get "/health/ready", HealthController, :ready
  end
end
