defmodule StarciteWeb.LifecycleChannelTest do
  use ExUnit.Case, async: false
  import Phoenix.ChannelTest

  alias Starcite.Auth.Principal
  alias Starcite.WritePath
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.LifecycleChannel

  @endpoint StarciteWeb.Endpoint

  setup do
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})
    :ok
  end

  test "pushes created sessions for the authenticated tenant" do
    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-1", %{auth: auth_context()})
      |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})

    session_id = unique_id("ses")

    assert {:ok, _session} =
             WritePath.create_session(
               id: session_id,
               tenant_id: "acme",
               title: "Draft",
               metadata: %{workflow: "contract"}
             )

    assert_push "lifecycle", %{
      event: %{
        kind: "session.created",
        session_id: ^session_id,
        tenant_id: "acme",
        title: "Draft",
        metadata: %{workflow: "contract"},
        created_at: created_at
      }
    }

    assert String.ends_with?(created_at, "Z")

    assert {:ok, _reply} =
             append_event(session_id, %{
               type: "content",
               payload: %{text: "ignore me"},
               actor: "agent:test"
             })

    Process.sleep(50)
    refute_received %Phoenix.Socket.Message{event: "lifecycle"}
  end

  test "rejects lifecycle joins for session-scoped tokens" do
    assert {:error, %{reason: "forbidden_session"}} =
             socket(StarciteWeb.UserSocket, "client-2", %{
               auth: auth_context(session_id: "ses-locked")
             })
             |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})
  end

  test "emits token_expired when the auth lifetime expires" do
    Process.flag(:trap_exit, true)

    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-3", %{
        auth: auth_context(expires_at: System.system_time(:second) + 1)
      })
      |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})

    Process.sleep(1_100)
    assert_push "token_expired", %{reason: "token_expired"}
    assert_receive {:EXIT, _pid, {:shutdown, :token_expired}}
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  defp auth_context(overrides \\ []) do
    base = %Context{
      kind: :jwt,
      principal: %Principal{tenant_id: "acme", id: "user-1", type: :user},
      scopes: ["session:read"],
      session_id: nil,
      expires_at: nil
    }

    struct!(base, overrides)
  end

  defp append_event(id, event, opts \\ [])
       when is_binary(id) and is_map(event) and is_list(opts) do
    producer_id = Map.get(event, :producer_id, "writer:test")

    enriched_event =
      event
      |> Map.put_new(:producer_id, producer_id)
      |> Map.put_new_lazy(:producer_seq, fn -> next_producer_seq(id, producer_id) end)

    WritePath.append_event(id, enriched_event, opts)
  end

  defp next_producer_seq(session_id, producer_id)
       when is_binary(session_id) and is_binary(producer_id) do
    counters = Process.get(:producer_seq_counters, %{})
    key = {session_id, producer_id}
    seq = Map.get(counters, key, 0) + 1
    Process.put(:producer_seq_counters, Map.put(counters, key, seq))
    seq
  end
end
