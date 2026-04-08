defmodule StarciteWeb.Router do
  use StarciteWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/v1", StarciteWeb do
    pipe_through [:api]

    post "/sessions", SessionController, :create
    get "/sessions", SessionController, :index
    patch "/sessions/:id", SessionController, :update
    post "/sessions/:id/append", SessionController, :append
  end
end
