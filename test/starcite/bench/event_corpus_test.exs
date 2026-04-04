defmodule Starcite.Bench.EventCorpusTest do
  use ExUnit.Case, async: true

  alias Starcite.Bench.EventCorpus
  alias Starcite.Storage.EventArchive.S3.Schema

  defmodule FakeClient do
    def list_keys(_config, "starcite/events/v1/") do
      {:ok, ["starcite/events/v1/tenant/session/1.ndjson"]}
    end

    def get_object(_config, "starcite/events/v1/tenant/session/1.ndjson") do
      events = [
        %{
          seq: 1,
          type: "agent.streaming.chunk",
          payload: %{
            "text" => "hello world",
            "parts" => [%{"type" => "text", "text" => "abc"}]
          },
          actor: "assistant",
          producer_id: "writer-1",
          producer_seq: 1,
          source: "openai",
          metadata: %{"starcite_principal" => %{"tenant_id" => "bench"}, "shape" => "chunk"},
          refs: %{},
          idempotency_key: "msg-1",
          tenant_id: "bench",
          inserted_at: "2026-04-04T10:00:00"
        },
        %{
          seq: 2,
          type: "agent.done",
          payload: %{"usage" => %{"input_tokens" => 10, "output_tokens" => 5}},
          actor: "assistant",
          producer_id: "writer-1",
          producer_seq: 2,
          source: "openai",
          metadata: %{"shape" => "done"},
          refs: %{"to_seq" => 1},
          idempotency_key: "msg-2",
          tenant_id: "bench",
          inserted_at: "2026-04-04T10:00:01"
        }
      ]

      {:ok, {Schema.encode_event_chunk(events), nil}}
    end
  end

  test "sanitize_event_template/1 masks nested strings and strips archival-only fields" do
    template =
      EventCorpus.sanitize_event_template(%{
        type: "agent.streaming.chunk",
        payload: %{"text" => "hello", "parts" => [%{"text" => "abc"}]},
        source: "openai",
        metadata: %{"starcite_principal" => %{"tenant_id" => "bench"}, "shape" => "chunk"},
        producer_id: "writer-1",
        producer_seq: 1,
        seq: 1,
        tenant_id: "bench",
        inserted_at: "2026-04-04T10:00:00"
      })

    assert template == %{
             type: "agent.streaming.chunk",
             payload: %{"text" => "xxxxx", "parts" => [%{"text" => "xxx"}]},
             source: "xxxxxx",
             metadata: %{"shape" => "xxxxx"}
           }
  end

  test "sample_archive/2 builds a sanitized corpus document" do
    config = %{client_mod: FakeClient, prefix: "starcite"}

    assert {:ok, document} =
             EventCorpus.sample_archive(config,
               object_limit: 1,
               event_limit: 2,
               sample_fun: fn items, count -> Enum.take(items, count) end
             )

    assert document.schema_version == 1
    assert document.sampled_object_count == 1
    assert document.sampled_event_count == 2
    assert document.type_counts == %{"agent.done" => 1, "agent.streaming.chunk" => 1}

    assert document.events == [
             %{
               type: "agent.streaming.chunk",
               payload: %{
                 "text" => "xxxxxxxxxxx",
                 "parts" => [%{"type" => "xxxx", "text" => "xxx"}]
               },
               source: "xxxxxx",
               metadata: %{"shape" => "xxxxx"}
             },
             %{
               type: "agent.done",
               payload: %{"usage" => %{"input_tokens" => 10, "output_tokens" => 5}},
               source: "xxxxxx",
               metadata: %{"shape" => "xxxx"}
             }
           ]
  end
end
