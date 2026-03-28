defmodule StarciteWeb.TailChannelTest do
  use ExUnit.Case, async: false
  import Phoenix.ChannelTest

  alias Starcite.Auth.Principal
  alias Starcite.WritePath
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.TailChannel

  @endpoint StarciteWeb.Endpoint

  setup do
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})
    :ok
  end

  test "replays from the requested cursor and streams live events" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    {:ok, _} =
      append_event(session_id, %{
        type: "content",
        payload: %{text: "replay"},
        actor: "agent:test"
      })

    {:ok, _, socket} =
      socket(StarciteWeb.UserSocket, "client-1", %{auth: auth_context()})
      |> subscribe_and_join(TailChannel, "tail:#{session_id}", %{})

    assert_push "events", %{events: [%{seq: 1} = replay_event]}
    assert replay_event.type == "content"
    assert replay_event.payload == %{text: "replay"}
    assert replay_event.cursor == 1
    refute Map.has_key?(replay_event, :epoch)
    assert String.ends_with?(replay_event.inserted_at, "Z")

    {:ok, _} =
      append_event(session_id, %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:test"
      })

    assert_push "events", %{events: [%{seq: 2} = live_event]}
    assert live_event.type == "state"
    assert live_event.payload == %{state: "running"}
    assert live_event.cursor == 2
    refute Map.has_key?(live_event, :epoch)
    assert String.ends_with?(live_event.inserted_at, "Z")
    assert socket.topic == "tail:#{session_id}"
  end

  test "batches replay pushes when batch_size is greater than one" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for n <- 1..3 do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "replay-#{n}"},
          actor: "agent:test"
        })
    end

    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-2", %{auth: auth_context()})
      |> subscribe_and_join(TailChannel, "tail:#{session_id}", %{
        "batch_size" => 2
      })

    assert_push "events", %{events: [%{seq: 1}, %{seq: 2}]}
    assert_push "events", %{events: [%{seq: 3}]}
  end

  test "emits token_expired when the auth lifetime expires" do
    Process.flag(:trap_exit, true)
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-3", %{
        auth: auth_context(expires_at: System.system_time(:second) + 1, session_id: session_id)
      })
      |> subscribe_and_join(TailChannel, "tail:#{session_id}", %{})

    Process.sleep(1_100)
    assert_push "token_expired", %{reason: "token_expired"}
    assert_receive {:EXIT, _pid, {:shutdown, :token_expired}}
  end

  test "rejects joins outside the scoped session" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    assert {:error, %{reason: "forbidden_session"}} =
             socket(StarciteWeb.UserSocket, "client-4", %{
               auth: auth_context(session_id: "other-session")
             })
             |> subscribe_and_join(TailChannel, "tail:#{session_id}", %{})
  end

  test "accepts seq cursor for resume" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-5", %{auth: auth_context()})
      |> subscribe_and_join(TailChannel, "tail:#{session_id}", %{
        "cursor" => 1
      })

    assert_push "events", %{events: [%{seq: 2, cursor: 2, payload: %{text: "two"}}]}
  end

  test "rejects non-seq cursor and invalid batch size shapes" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    assert {:error, %{reason: "invalid_cursor"}} =
             socket(StarciteWeb.UserSocket, "client-7", %{auth: auth_context()})
             |> subscribe_and_join(TailChannel, "tail:#{session_id}", %{"cursor" => nil})

    assert {:error, %{reason: "invalid_cursor"}} =
             socket(StarciteWeb.UserSocket, "client-6", %{auth: auth_context()})
             |> subscribe_and_join(TailChannel, "tail:#{session_id}", %{
               "cursor" => %{"epoch" => 0, "seq" => 0}
             })

    assert {:error, %{reason: "invalid_tail_batch_size"}} =
             socket(StarciteWeb.UserSocket, "client-8", %{auth: auth_context()})
             |> subscribe_and_join(TailChannel, "tail:#{session_id}", %{"batch_size" => "2"})
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
