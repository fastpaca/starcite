defmodule Starcite.DataPlane.SessionDiscovery do
  @moduledoc """
  Service-level session lifecycle discovery contract over PubSub.

  Topic:
    - `"session_discovery"`

  Message:
    - `{:session_discovery, update_map}`
  """

  alias Phoenix.PubSub
  alias Starcite.Observability.Telemetry

  @topic "session_discovery"

  @type lifecycle_kind :: :session_created | :session_frozen | :session_hydrated
  @type lifecycle_state :: :active | :frozen

  @type t :: %{
          required(:version) => 1,
          required(:kind) => lifecycle_kind(),
          required(:state) => lifecycle_state(),
          required(:session_id) => String.t(),
          required(:tenant_id) => String.t(),
          required(:occurred_at) => String.t()
        }

  @type message :: {:session_discovery, t()}

  @spec topic() :: String.t()
  def topic, do: @topic

  @spec publish_created(String.t(), String.t(), keyword()) :: :ok
  def publish_created(session_id, tenant_id, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_list(opts) do
    :ok = Telemetry.session_create(session_id, tenant_id)
    publish(:session_created, :active, session_id, tenant_id, occurred_at(opts))
  end

  @spec publish_frozen(String.t(), String.t(), keyword()) :: :ok
  def publish_frozen(session_id, tenant_id, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_list(opts) do
    publish(:session_frozen, :frozen, session_id, tenant_id, occurred_at(opts))
  end

  @spec publish_hydrated(String.t(), String.t(), keyword()) :: :ok
  def publish_hydrated(session_id, tenant_id, opts \\ [])
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_list(opts) do
    publish(:session_hydrated, :active, session_id, tenant_id, occurred_at(opts))
  end

  defp publish(kind, state, session_id, tenant_id, occurred_at)
       when kind in [:session_created, :session_frozen, :session_hydrated] and
              state in [:active, :frozen] and is_binary(session_id) and session_id != "" and
              is_binary(tenant_id) and tenant_id != "" and is_binary(occurred_at) and
              occurred_at != "" do
    PubSub.broadcast(
      Starcite.PubSub,
      @topic,
      {:session_discovery,
       %{
         version: 1,
         kind: kind,
         state: state,
         session_id: session_id,
         tenant_id: tenant_id,
         occurred_at: occurred_at
       }}
    )

    :ok
  end

  defp occurred_at(opts) when is_list(opts) do
    case Keyword.get(opts, :occurred_at) do
      value when is_binary(value) and value != "" ->
        value

      %DateTime{} = value ->
        value
        |> DateTime.truncate(:second)
        |> DateTime.to_iso8601()

      %NaiveDateTime{} = value ->
        value
        |> DateTime.from_naive!("Etc/UTC")
        |> DateTime.truncate(:second)
        |> DateTime.to_iso8601()

      _ ->
        DateTime.utc_now()
        |> DateTime.truncate(:second)
        |> DateTime.to_iso8601()
    end
  end
end
