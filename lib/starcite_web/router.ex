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
    get "/sessions/:id/projections", SessionController, :list_projections
    post "/sessions/:id/projections", SessionController, :put_projections
    get "/sessions/:id/projections/:item_id", SessionController, :show_projection
    delete "/sessions/:id/projections/:item_id", SessionController, :delete_projection

    get "/sessions/:id/projections/:item_id/versions",
        SessionController,
        :list_projection_versions

    get "/sessions/:id/projections/:item_id/versions/:version",
        SessionController,
        :show_projection_version

    post "/sessions/:id/archive", SessionController, :archive
    post "/sessions/:id/unarchive", SessionController, :unarchive
  end
end
