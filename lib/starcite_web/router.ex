defmodule StarciteWeb.Router do
  use StarciteWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/v1", StarciteWeb do
    pipe_through [:api]

    post "/sessions", SessionController, :create
    get "/sessions", SessionController, :index
    get "/sessions/:id", SessionController, :show
    patch "/sessions/:id", SessionController, :update
    post "/sessions/:id/append", SessionController, :append
    post "/sessions/:id/archive", SessionController, :archive
    post "/sessions/:id/unarchive", SessionController, :unarchive
  end
end
