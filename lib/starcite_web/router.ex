defmodule StarciteWeb.Router do
  use StarciteWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
    plug OpenApiSpex.Plug.PutApiSpec, module: StarciteWeb.ApiSpec
  end

  scope "/v1", StarciteWeb do
    pipe_through :api

    post "/sessions", SessionController, :create
    post "/sessions/:id/append", SessionController, :append
    get "/sessions/:id/tail", TailController, :tail
  end

  scope "/v1" do
    pipe_through :api

    get "/openapi", OpenApiSpex.Plug.RenderSpec, []
  end

  scope "/v1" do
    get "/docs", OpenApiSpex.Plug.SwaggerUI, path: "/v1/openapi"
  end

  scope "/", StarciteWeb do
    get "/health/live", HealthController, :live
    get "/health/ready", HealthController, :ready
  end
end
