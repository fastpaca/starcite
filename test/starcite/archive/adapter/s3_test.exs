defmodule Starcite.Archive.Adapter.S3Test do
  use ExUnit.Case, async: false

  alias Starcite.Archive.Adapter.S3

  defmodule FakeClient do
    @store __MODULE__.Store

    def store_name, do: @store

    def get_object(%{bucket: bucket}, key) do
      case Agent.get(@store, fn state -> state |> Map.get(bucket, %{}) |> Map.get(key) end) do
        nil -> {:ok, :not_found}
        %{body: body, etag: etag} -> {:ok, {body, etag}}
      end
    end

    def put_object(%{bucket: bucket}, key, body, opts \\ []) do
      Agent.get_and_update(@store, fn state ->
        existing = state |> Map.get(bucket, %{}) |> Map.get(key)

        if precondition_failed?(existing, opts) do
          {{:error, :precondition_failed}, state}
        else
          bucket_state = Map.get(state, bucket, %{})
          etag = etag_for(body)

          next_state =
            Map.put(state, bucket, Map.put(bucket_state, key, %{body: body, etag: etag}))

          {:ok, next_state}
        end
      end)
    end

    def list_keys(%{bucket: bucket}, prefix) do
      keys =
        Agent.get(@store, fn state ->
          state
          |> Map.get(bucket, %{})
          |> Map.keys()
          |> Enum.filter(&String.starts_with?(&1, prefix))
          |> Enum.sort()
        end)

      {:ok, keys}
    end

    defp precondition_failed?(existing, opts) do
      if_none_match = Keyword.get(opts, :if_none_match)
      if_match = Keyword.get(opts, :if_match)

      cond do
        if_none_match == "*" and existing != nil -> true
        is_binary(if_match) and existing == nil -> true
        is_binary(if_match) and existing.etag != if_match -> true
        true -> false
      end
    end

    defp etag_for(body) when is_binary(body) do
      digest =
        :crypto.hash(:md5, body)
        |> Base.encode16(case: :lower)

      ~s("#{digest}")
    end
  end

  setup do
    start_supervised!(%{
      id: FakeClient.store_name(),
      start: {Agent, :start_link, [fn -> %{} end, [name: FakeClient.store_name()]]}
    })

    session_prefix = "s3-test-#{System.unique_integer([:positive, :monotonic])}"

    start_supervised!(
      {S3, bucket: "archive-test", prefix: session_prefix, client_mod: FakeClient}
    )

    :ok
  end

  test "write_events is idempotent and reads across chunk boundaries" do
    session_id = "ses-s3-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    rows = event_rows(session_id, inserted_at, 1..260)

    assert {:ok, 260} = S3.write_events(rows)
    assert {:ok, 0} = S3.write_events(rows)

    assert {:ok, events} = S3.read_events(session_id, 255, 258)
    assert Enum.map(events, & &1.seq) == [255, 256, 257, 258]
    assert Enum.all?(events, &is_binary(&1.inserted_at))
  end

  test "upserts and lists sessions with metadata filtering and cursor paging" do
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)

    assert :ok =
             S3.upsert_session(%{
               id: "ses-a",
               title: "A",
               metadata: %{"tenant_id" => "acme"},
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-b",
               title: "B",
               metadata: %{"tenant_id" => "acme"},
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-c",
               title: "C",
               metadata: %{"tenant_id" => "beta"},
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-a",
               title: "A-overwrite-attempt",
               metadata: %{"tenant_id" => "wrong"},
               created_at: created_at
             })

    assert {:ok, page_1} =
             S3.list_sessions(%{limit: 1, cursor: nil, metadata: %{"tenant_id" => "acme"}})

    assert length(page_1.sessions) == 1
    assert is_binary(page_1.next_cursor)

    assert {:ok, page_2} =
             S3.list_sessions(%{
               limit: 2,
               cursor: page_1.next_cursor,
               metadata: %{"tenant_id" => "acme"}
             })

    listed_ids =
      (page_1.sessions ++ page_2.sessions)
      |> Enum.map(& &1.id)
      |> Enum.sort()

    assert listed_ids == ["ses-a", "ses-b"]

    session_a =
      (page_1.sessions ++ page_2.sessions)
      |> Enum.find(&(&1.id == "ses-a"))

    assert session_a.title == "A"
    assert session_a.metadata == %{"tenant_id" => "acme"}

    assert {:ok, by_ids} =
             S3.list_sessions_by_ids(
               ["ses-c", "ses-a", "missing"],
               %{limit: 10, cursor: nil, metadata: %{}}
             )

    assert by_ids.sessions |> Enum.map(& &1.id) |> Enum.sort() == ["ses-a", "ses-c"]
  end

  defp event_rows(session_id, inserted_at, seqs) do
    Enum.map(seqs, fn seq ->
      %{
        session_id: session_id,
        seq: seq,
        type: "content",
        payload: %{n: seq},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: seq,
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: inserted_at
      }
    end)
  end
end
