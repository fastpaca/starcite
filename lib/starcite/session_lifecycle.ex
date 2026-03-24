defmodule Starcite.SessionLifecycle do
  @moduledoc """
  Live tenant-scoped session lifecycle fanout over Phoenix PubSub.

  This boundary is intentionally ephemeral. It emits session lifecycle
  notifications owned by Starcite itself so authenticated socket consumers can
  react in real time without coupling orchestration to application event types.
  """

  alias Phoenix.PubSub
  alias Starcite.Session
  alias Starcite.Time

  @spec topic(String.t()) :: String.t()
  def topic(tenant_id) when is_binary(tenant_id) and tenant_id != "" do
    "lifecycle:" <> tenant_id
  end

  @spec broadcast_created(Session.t()) :: :ok
  def broadcast_created(%Session{} = session) do
    publish(
      session.tenant_id,
      %{
        kind: "session.created",
        session_id: session.id,
        tenant_id: session.tenant_id,
        title: session.title,
        metadata: session.metadata,
        created_at: Time.iso8601_utc!(session.inserted_at)
      }
    )
  end

  defp publish(tenant_id, event)
       when is_binary(tenant_id) and tenant_id != "" and is_map(event) do
    PubSub.broadcast(Starcite.PubSub, topic(tenant_id), {:session_lifecycle, event})
  end
end
