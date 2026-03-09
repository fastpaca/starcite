defmodule Starcite.DataPlane.TailBroadcast do
  @moduledoc """
  Live tail broadcast contract for Phoenix tail channels.

  Topic:
    - `"tail:<session_id>"`

  Event:
    - `"tail_event"`
  """

  alias Starcite.Auth.Principal
  alias Starcite.Session
  alias Starcite.Session.Event

  @topic_prefix "tail:"
  @event_name "tail_event"

  @type principal_metadata :: %{
          optional(String.t()) => term()
        }

  @type t :: %{
          required(:version) => 1,
          required(:session_id) => String.t(),
          required(:tenant_id) => String.t(),
          required(:seq) => pos_integer(),
          required(:last_seq) => pos_integer(),
          required(:type) => String.t(),
          required(:actor) => String.t(),
          optional(:source) => String.t() | nil,
          required(:inserted_at) => NaiveDateTime.t() | DateTime.t(),
          optional(:principal) => principal_metadata() | nil,
          optional(:owner_principal) => Principal.t() | nil,
          required(:event) => Event.t()
        }

  @spec topic(String.t()) :: String.t()
  def topic(session_id) when is_binary(session_id) and session_id != "" do
    @topic_prefix <> session_id
  end

  @spec event_name() :: String.t()
  def event_name, do: @event_name

  @spec payload(Session.t(), Event.t(), pos_integer()) :: t()
  def payload(
        %Session{
          id: session_id,
          tenant_id: tenant_id,
          creator_principal: owner_principal
        },
        %{
          seq: seq,
          type: type,
          actor: actor,
          source: source,
          metadata: metadata,
          inserted_at: inserted_at
        } = event,
        last_seq
      )
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_integer(last_seq) and last_seq > 0 do
    %{
      version: 1,
      session_id: session_id,
      tenant_id: tenant_id,
      owner_principal: owner_principal,
      seq: seq,
      last_seq: last_seq,
      type: type,
      actor: actor,
      source: source,
      inserted_at: inserted_at,
      principal: principal_metadata(metadata),
      event: event
    }
  end

  defp principal_metadata(metadata) when is_map(metadata) do
    case Map.get(metadata, "starcite_principal") do
      value when is_map(value) -> value
      _ -> nil
    end
  end

  defp principal_metadata(_metadata), do: nil
end
