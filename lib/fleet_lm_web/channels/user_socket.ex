defmodule FleetLMWeb.UserSocket do
  use Phoenix.Socket

  # Channels
  channel "conversation:*", FleetLMWeb.ConversationChannel

  @impl true
  def connect(_params, socket, _connect_info) do
    {:ok, socket}
  end

  @impl true
  def id(_socket), do: nil
end
