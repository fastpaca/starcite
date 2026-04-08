defmodule StarciteWeb.LifecycleChannelTest do
  use ExUnit.Case, async: false
  import Phoenix.ChannelTest

  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.SessionQuorum
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
      socket(StarciteWeb.UserSocket, "client-1", %{auth: service_auth_context()})
      |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})

    session_id = unique_id("ses")

    assert {:ok, _session} =
             WritePath.create_session(
               id: session_id,
               tenant_id: "acme",
               title: "Draft",
               metadata: %{workflow: "contract"}
             )

    assert_push "lifecycle", %{event: event}
    assert event == %{kind: "session.activated", session_id: session_id, tenant_id: "acme"}

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

  test "pushes freeze and hydrate lifecycle events for the authenticated tenant" do
    original_idle_timeout = Application.get_env(:starcite, :session_log_idle_timeout_ms)

    original_idle_check_interval =
      Application.get_env(:starcite, :session_log_idle_check_interval_ms)

    on_exit(fn ->
      restore_app_env(:session_log_idle_timeout_ms, original_idle_timeout)
      restore_app_env(:session_log_idle_check_interval_ms, original_idle_check_interval)
    end)

    Application.put_env(:starcite, :session_log_idle_timeout_ms, 120)
    Application.put_env(:starcite, :session_log_idle_check_interval_ms, 20)

    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-freeze", %{auth: service_auth_context()})
      |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})

    session_id = unique_id("ses-freeze")

    assert {:ok, _session} =
             WritePath.create_session(
               id: session_id,
               tenant_id: "acme"
             )

    assert_push "lifecycle", %{event: event}
    assert event == %{kind: "session.activated", session_id: session_id, tenant_id: "acme"}

    assert_push "lifecycle", %{
      event: %{
        kind: "session.created",
        session_id: ^session_id,
        tenant_id: "acme"
      }
    }

    [{pid, _value}] = Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, session_id)
    ref = Process.monitor(pid)

    assert_push "lifecycle", %{event: event}, 2_000
    assert event == %{kind: "session.freezing", session_id: session_id, tenant_id: "acme"}

    assert_push "lifecycle", %{event: event}, 2_000
    assert event == %{kind: "session.frozen", session_id: session_id, tenant_id: "acme"}

    assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 2_000
    assert reason in [:normal, :shutdown]

    eventually(
      fn ->
        assert [] == Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, session_id)
      end,
      timeout: 1_000
    )

    assert {:ok, _session} = SessionQuorum.get_session(session_id)

    assert_push "lifecycle", %{event: event}, 1_000
    assert event == %{kind: "session.hydrating", session_id: session_id, tenant_id: "acme"}

    assert_push "lifecycle", %{event: event}, 1_000
    assert event == %{kind: "session.activated", session_id: session_id, tenant_id: "acme"}
  end

  test "pushes session.updated when mutable header fields change" do
    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-update", %{auth: service_auth_context()})
      |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})

    session_id = unique_id("ses-update")

    assert {:ok, _session} =
             WritePath.create_session(
               id: session_id,
               tenant_id: "acme",
               title: "Draft",
               metadata: %{workflow: "contract"}
             )

    assert_push "lifecycle", %{event: %{kind: "session.activated"}}
    assert_push "lifecycle", %{event: %{kind: "session.created", version: 1}}

    assert {:ok, _session} =
             WritePath.update_session(session_id, %{
               title: "Final",
               metadata: %{"summary" => "Generated"}
             })

    assert_push "lifecycle", %{
      event: %{
        kind: "session.updated",
        session_id: ^session_id,
        tenant_id: "acme",
        title: "Final",
        metadata: %{"workflow" => "contract", "summary" => "Generated"},
        updated_at: updated_at,
        version: 2
      }
    }

    assert String.ends_with?(updated_at, "Z")
  end

  test "does not push lifecycle events for another tenant" do
    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-tenant-scope", %{auth: service_auth_context()})
      |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})

    other_principal = %Principal{tenant_id: "other", id: "svc-other", type: :service}

    assert {:ok, _session} =
             WritePath.create_session(
               id: unique_id("ses-other"),
               tenant_id: "other",
               creator_principal: other_principal
             )

    Process.sleep(100)
    refute_received %Phoenix.Socket.Message{event: "lifecycle"}
  end

  test "pushes archive lifecycle events for the authenticated tenant" do
    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-archive", %{auth: service_auth_context()})
      |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})

    session_id = unique_id("ses-archive")

    assert {:ok, _session} =
             WritePath.create_session(
               id: session_id,
               tenant_id: "acme"
             )

    assert_push "lifecycle", %{event: %{kind: "session.activated", session_id: ^session_id}}
    assert_push "lifecycle", %{event: %{kind: "session.created", session_id: ^session_id}}

    assert {:ok, archived} = WritePath.archive_session(session_id)
    assert archived.archived == true

    assert_push "lifecycle", %{event: event}

    assert event == %{
             kind: "session.archived",
             session_id: session_id,
             tenant_id: "acme",
             archived: true
           }

    assert {:ok, unarchived} = WritePath.unarchive_session(session_id)
    assert unarchived.archived == false

    assert_push "lifecycle", %{event: event}

    assert event == %{
             kind: "session.unarchived",
             session_id: session_id,
             tenant_id: "acme",
             archived: false
           }
  end

  test "rejects lifecycle joins for non-service jwt principals" do
    assert {:error, %{reason: "forbidden"}} =
             socket(StarciteWeb.UserSocket, "client-user", %{
               auth: user_auth_context()
             })
             |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})
  end

  test "rejects lifecycle joins for session-scoped tokens" do
    assert {:error, %{reason: "forbidden_session"}} =
             socket(StarciteWeb.UserSocket, "client-2", %{
               auth: service_auth_context(session_id: "ses-locked")
             })
             |> subscribe_and_join(LifecycleChannel, "lifecycle", %{})
  end

  test "emits token_expired when the auth lifetime expires" do
    Process.flag(:trap_exit, true)

    {:ok, _, _socket} =
      socket(StarciteWeb.UserSocket, "client-3", %{
        auth: service_auth_context(expires_at: System.system_time(:second) + 1)
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

  defp service_auth_context(overrides \\ []) do
    base = %Context{
      kind: :jwt,
      principal: %Principal{tenant_id: "acme", id: "svc-backend", type: :service},
      scopes: ["session:read"],
      session_id: nil,
      expires_at: nil
    }

    struct!(base, overrides)
  end

  defp user_auth_context(overrides \\ []) do
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

  defp eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 1_000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    try do
      fun.()
    rescue
      _ ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        else
          fun.()
        end
    end
  end

  defp restore_app_env(key, nil), do: Application.delete_env(:starcite, key)
  defp restore_app_env(key, value), do: Application.put_env(:starcite, key, value)
end
