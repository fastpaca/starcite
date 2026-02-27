defmodule Starcite.DataPlane.SessionStoreTest do
  use ExUnit.Case, async: false

  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.SessionStore
  alias Starcite.Session

  setup do
    original_adapter = Application.get_env(:starcite, :archive_adapter)
    Application.put_env(:starcite, :archive_adapter, Starcite.Archive.TestAdapter)
    SessionStore.clear()

    on_exit(fn ->
      SessionStore.clear()
      Application.put_env(:starcite, :archive_adapter, original_adapter)
    end)

    :ok
  end

  test "stores and loads sessions by id" do
    session = Session.new("ses-store-1", creator_principal: principal())

    assert :ok = SessionStore.put_session(session)
    assert {:ok, loaded} = SessionStore.get_session("ses-store-1")
    assert loaded.id == "ses-store-1"
    assert loaded.last_seq == 0
  end

  test "replaces existing session snapshot" do
    session = Session.new("ses-store-2", creator_principal: principal())
    assert :ok = SessionStore.put_session(session)

    {:appended, updated, _event} =
      Session.append_event(session, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        metadata: %{},
        refs: %{},
        source: nil,
        idempotency_key: nil
      })

    assert :ok = SessionStore.put_session(updated)
    assert {:ok, loaded} = SessionStore.get_session("ses-store-2")
    assert loaded.last_seq == 1
  end

  test "deletes sessions" do
    assert :ok =
             SessionStore.put_session(Session.new("ses-store-3", creator_principal: principal()))

    assert {:ok, _session} = SessionStore.get_session("ses-store-3")

    assert :ok = SessionStore.delete_session("ses-store-3")
    assert {:error, :session_not_found} = SessionStore.get_session("ses-store-3")
  end

  test "reports ids, size, and memory" do
    assert :ok =
             SessionStore.put_session(Session.new("ses-store-a", creator_principal: principal()))

    assert :ok =
             SessionStore.put_session(Session.new("ses-store-b", creator_principal: principal()))

    assert SessionStore.size() == 2

    ids = SessionStore.session_ids() |> Enum.sort()
    assert ids == ["ses-store-a", "ses-store-b"]

    assert is_integer(SessionStore.memory_bytes())
    assert SessionStore.memory_bytes() > 0
  end

  defp principal do
    %Principal{tenant_id: "acme", id: "user-1", type: :user}
  end
end
