defmodule StarciteWeb.Router do
  use StarciteWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
    plug StarciteWeb.Plugs.ApiAuth
  end

  scope "/v1", StarciteWeb do
    pipe_through :api

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
