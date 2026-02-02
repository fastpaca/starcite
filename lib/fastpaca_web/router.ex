defmodule FastpacaWeb.Router do
  use FastpacaWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/v1", FastpacaWeb do
    pipe_through :api

    # Conversation lifecycle
    put "/conversations/:id", ConversationController, :upsert
    get "/conversations/:id", ConversationController, :show
    delete "/conversations/:id", ConversationController, :delete

    # Messages
    post "/conversations/:id/messages", ConversationController, :append
    get "/conversations/:id/tail", ConversationController, :tail
    get "/conversations/:id/messages", ConversationController, :replay
  end

  scope "/", FastpacaWeb do
    get "/health/live", HealthController, :live
    get "/health/ready", HealthController, :ready
  end
end
