defmodule Starcite.ProjectionTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.SessionStore
  alias Starcite.Storage.SessionCatalog
  alias Starcite.{Projection, ReadPath, Session, WritePath}

  setup do
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})
    :ok
  end

  test "composed replay skips a covered interval when resuming from inside it" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three", "four"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, _item} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 1,
               from_seq: 2,
               to_seq: 3,
               payload: %{"text" => "two three"},
               metadata: %{}
             })

    assert {:ok, [%{seq: 4, payload: %{text: "four"}}]} =
             ReadPath.replay_from_cursor(session_id, 2, 10, :composed)
  end

  test "default replay uses the composed view" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three", "four"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, _item} =
             Projection.put_item(session_id, %{
               item_id: "msg-default",
               version: 1,
               from_seq: 2,
               to_seq: 3,
               payload: %{"text" => "two three"},
               metadata: %{}
             })

    assert {:ok, events} = ReadPath.replay_from_cursor(session_id, 0, 10)

    assert [
             %{seq: 1, type: "content", payload: %{text: "one"}},
             %{seq: 3, from_seq: 2, type: "projection", item_id: "msg-default", version: 1},
             %{seq: 4, type: "content", payload: %{text: "four"}}
           ] = events
  end

  test "put_items supports coherent rewrites of multiple existing projection items" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, _} =
             Projection.put_items(session_id, [
               %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               },
               %{
                 item_id: "msg-2",
                 version: 1,
                 from_seq: 2,
                 to_seq: 2,
                 payload: %{"text" => "two"},
                 metadata: %{}
               }
             ])

    assert {:ok, _} =
             Projection.put_items(session_id, [
               %{
                 item_id: "msg-1",
                 version: 2,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               },
               %{
                 item_id: "msg-2",
                 version: 2,
                 from_seq: 3,
                 to_seq: 3,
                 payload: %{"text" => "three"},
                 metadata: %{}
               }
             ])

    assert {:ok, items} = Projection.latest_items(session_id)

    assert Enum.map(items, fn item -> {item.item_id, item.version, item.from_seq, item.to_seq} end) ==
             [
               {"msg-1", 2, 1, 2},
               {"msg-2", 2, 3, 3}
             ]
  end

  test "put_item rejects a new latest version that overlaps another item's latest interval" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three", "four"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 1,
               from_seq: 1,
               to_seq: 1,
               payload: %{"text" => "one"},
               metadata: %{}
             })

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-2",
               version: 1,
               from_seq: 3,
               to_seq: 4,
               payload: %{"text" => "three four"},
               metadata: %{}
             })

    assert {:error, :projection_item_overlap} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 2,
               from_seq: 2,
               to_seq: 3,
               payload: %{"text" => "two three"},
               metadata: %{}
             })
  end

  test "put_items rejects overlapping intervals within the same batch" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three", "four"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:error, :projection_item_overlap} =
             Projection.put_items(session_id, [
               %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               },
               %{
                 item_id: "msg-2",
                 version: 1,
                 from_seq: 2,
                 to_seq: 3,
                 payload: %{"text" => "two three"},
                 metadata: %{}
               }
             ])
  end

  test "put_item allows a new version for the same item to replace its interval" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, %{version: 1}} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 1,
               from_seq: 1,
               to_seq: 1,
               payload: %{"text" => "one"},
               metadata: %{}
             })

    assert {:ok, %{version: 2}} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 2,
               from_seq: 1,
               to_seq: 2,
               payload: %{"text" => "one two"},
               metadata: %{}
             })

    assert {:ok, %{version: 2, from_seq: 1, to_seq: 2}} =
             Projection.get_item(session_id, "msg-1")

    assert {:ok, %{version: 1, from_seq: 1, to_seq: 1}} =
             Projection.get_item_version(session_id, "msg-1", 1)
  end

  test "archived-safe projection writes persist to the session catalog" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three", "four"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, _items} =
             Projection.put_items(session_id, [
               %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               },
               %{
                 item_id: "msg-2",
                 version: 1,
                 from_seq: 3,
                 to_seq: 4,
                 payload: %{"text" => "three four"},
                 metadata: %{}
               }
             ])

    assert {:ok, %{archived_seq: 4}} = WritePath.ack_archived(session_id, 4)

    assert {:ok, catalog_session} = SessionCatalog.get_session(session_id)

    assert [%{item_id: "msg-1"}, %{item_id: "msg-2"}] =
             Session.latest_projection_items(catalog_session)

    :ok = SessionStore.clear()

    assert {:ok, [%{item_id: "msg-1"}, %{item_id: "msg-2"}]} = Projection.latest_items(session_id)
  end

  test "archived-safe projection versions remain readable after session cache eviction" do
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

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 1,
               from_seq: 1,
               to_seq: 2,
               payload: %{"text" => "one two"},
               metadata: %{}
             })

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 2,
               from_seq: 1,
               to_seq: 2,
               payload: %{"text" => "one two"},
               metadata: %{"kind" => "message"}
             })

    assert {:ok, %{archived_seq: 2}} = WritePath.ack_archived(session_id, 2)

    :ok = SessionStore.clear()

    assert {:ok, %{item_id: "msg-1", version: 2, metadata: %{"kind" => "message"}}} =
             Projection.get_item(session_id, "msg-1")

    assert {:ok, [%{version: 1}, %{version: 2}]} = Projection.item_versions(session_id, "msg-1")

    assert {:ok, %{version: 1, payload: %{"text" => "one two"}}} =
             Projection.get_item_version(session_id, "msg-1", 1)
  end

  test "composed replay prefers latest versions and stitches projected and raw intervals" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three", "four", "five", "six"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 1,
               from_seq: 2,
               to_seq: 2,
               payload: %{"text" => "two"},
               metadata: %{}
             })

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 2,
               from_seq: 2,
               to_seq: 3,
               payload: %{"text" => "two three"},
               metadata: %{"kind" => "message"}
             })

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-2",
               version: 1,
               from_seq: 5,
               to_seq: 6,
               payload: %{"text" => "five six"},
               metadata: %{}
             })

    assert {:ok, events} = ReadPath.replay_from_cursor(session_id, 0, 10, :composed)

    assert [
             %{seq: 1, type: "content", payload: %{text: "one"}},
             %{seq: 3, from_seq: 2, type: "projection", item_id: "msg-1", version: 2},
             %{seq: 4, type: "content", payload: %{text: "four"}},
             %{seq: 6, from_seq: 5, type: "projection", item_id: "msg-2", version: 1}
           ] = events

    assert Enum.map(events, & &1.seq) == [1, 3, 4, 6]
    assert Enum.at(events, 1).payload == %{"text" => "two three"}
    assert Enum.at(events, 3).payload == %{"text" => "five six"}
  end

  test "composed replay counts a projection item as one replay item for limits" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three", "four", "five"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 1,
               from_seq: 2,
               to_seq: 4,
               payload: %{"text" => "two three four"},
               metadata: %{}
             })

    assert {:ok, events} = ReadPath.replay_from_cursor(session_id, 0, 3, :composed)

    assert Enum.map(events, & &1.seq) == [1, 4, 5]
    assert Enum.at(events, 1).type == "projection"
    assert Enum.at(events, 1).from_seq == 2
  end

  test "delete_item removes all versions for one item id" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for text <- ["one", "two", "three"] do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: text},
          actor: "agent:test"
        })
    end

    assert {:ok, _} =
             Projection.put_items(session_id, [
               %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               },
               %{
                 item_id: "msg-2",
                 version: 1,
                 from_seq: 2,
                 to_seq: 3,
                 payload: %{"text" => "two three"},
                 metadata: %{}
               }
             ])

    assert :ok = Projection.delete_item(session_id, "msg-1")
    assert {:error, :projection_item_not_found} = Projection.get_item(session_id, "msg-1")
    assert {:ok, [%{item_id: "msg-2"}]} = Projection.latest_items(session_id)
  end

  test "delete_item removes projection state from the session catalog" do
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

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 1,
               from_seq: 1,
               to_seq: 1,
               payload: %{"text" => "one"},
               metadata: %{}
             })

    assert {:ok, _} =
             Projection.put_item(session_id, %{
               item_id: "msg-1",
               version: 2,
               from_seq: 1,
               to_seq: 2,
               payload: %{"text" => "one two"},
               metadata: %{}
             })

    assert :ok = Projection.delete_item(session_id, "msg-1")

    assert {:ok, catalog_session} = SessionCatalog.get_session(session_id)
    assert [] = Session.latest_projection_items(catalog_session)

    :ok = SessionStore.clear()

    assert {:error, :projection_item_not_found} = Projection.get_item(session_id, "msg-1")
    assert {:ok, []} = Projection.latest_items(session_id)
    assert {:error, :projection_item_not_found} = Projection.item_versions(session_id, "msg-1")

    assert {:error, :projection_item_not_found} =
             Projection.get_item_version(session_id, "msg-1", 1)
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
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
