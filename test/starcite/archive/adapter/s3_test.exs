defmodule Starcite.Archive.Adapter.S3Test do
  use ExUnit.Case, async: false

  alias Starcite.Archive.Adapter.S3

  setup do
    bypass = Bypass.open()
    {:ok, store} = Agent.start_link(fn -> %{} end)

    Bypass.expect(bypass, fn conn ->
      handle_request(conn, store)
    end)

    session_prefix = "s3-test-#{System.unique_integer([:positive, :monotonic])}"

    {:ok, _pid} =
      start_supervised(
        {S3,
         bucket: "archive-test",
         prefix: session_prefix,
         endpoint: "http://localhost:#{bypass.port}",
         region: "auto",
         access_key_id: "test-access-key",
         secret_access_key: "test-secret-key",
         path_style: true,
         compressed: true}
      )

    {:ok, store: store}
  end

  test "write_events is idempotent and reads across chunk boundaries" do
    session_id = "ses-s3-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

    rows = event_rows(session_id, inserted_at, 1..260)

    assert {:ok, 260} = S3.write_events(rows)
    assert {:ok, 0} = S3.write_events(rows)

    assert {:ok, events} = S3.read_events(session_id, 255, 258)
    assert Enum.map(events, & &1.seq) == [255, 256, 257, 258]
    assert Enum.all?(events, &match?(%DateTime{}, &1.inserted_at))
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

  defp handle_request(conn, store) do
    conn = Plug.Conn.fetch_query_params(conn)

    case {conn.method, conn.path_info} do
      {"GET", [bucket]} ->
        handle_list_objects(conn, store, bucket)

      {"PUT", [bucket | key_parts]} ->
        handle_put_object(conn, store, bucket, Enum.join(key_parts, "/"))

      {"GET", [bucket | key_parts]} ->
        handle_get_object(conn, store, bucket, Enum.join(key_parts, "/"))

      {"HEAD", [bucket | key_parts]} ->
        handle_head_object(conn, store, bucket, Enum.join(key_parts, "/"))

      _ ->
        Plug.Conn.send_resp(conn, 400, "unsupported request")
    end
  end

  defp handle_put_object(conn, store, bucket, key)
       when is_binary(bucket) and bucket != "" and is_binary(key) and key != "" do
    {:ok, body, conn} = read_body(conn, "")
    existing = get_object(store, bucket, key)
    if_none_match = get_header(conn, "if-none-match")
    if_match = get_header(conn, "if-match")

    cond do
      if_none_match == "*" and existing != nil ->
        Plug.Conn.send_resp(conn, 412, "precondition failed")

      is_binary(if_match) and existing != nil and existing.etag != if_match ->
        Plug.Conn.send_resp(conn, 412, "precondition failed")

      is_binary(if_match) and existing == nil ->
        Plug.Conn.send_resp(conn, 412, "precondition failed")

      true ->
        etag = etag_for(body)
        put_object(store, bucket, key, body, etag)

        conn
        |> Plug.Conn.put_resp_header("etag", etag)
        |> Plug.Conn.send_resp(200, "")
    end
  end

  defp handle_get_object(conn, store, bucket, key)
       when is_binary(bucket) and bucket != "" and is_binary(key) and key != "" do
    case get_object(store, bucket, key) do
      nil ->
        Plug.Conn.send_resp(conn, 404, "not found")

      %{body: body, etag: etag} ->
        conn
        |> Plug.Conn.put_resp_header("etag", etag)
        |> Plug.Conn.send_resp(200, body)
    end
  end

  defp handle_head_object(conn, store, bucket, key)
       when is_binary(bucket) and bucket != "" and is_binary(key) and key != "" do
    case get_object(store, bucket, key) do
      nil ->
        Plug.Conn.send_resp(conn, 404, "")

      %{etag: etag} ->
        conn
        |> Plug.Conn.put_resp_header("etag", etag)
        |> Plug.Conn.send_resp(200, "")
    end
  end

  defp handle_list_objects(conn, store, bucket) when is_binary(bucket) and bucket != "" do
    prefix = Map.get(conn.query_params, "prefix", "")

    keys =
      list_keys(store, bucket)
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.sort()

    contents =
      Enum.map_join(keys, "", fn key ->
        "<Contents><Key>#{xml_escape(key)}</Key></Contents>"
      end)

    body = """
    <?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult>
      <IsTruncated>false</IsTruncated>
      #{contents}
    </ListBucketResult>
    """

    conn
    |> Plug.Conn.put_resp_content_type("application/xml")
    |> Plug.Conn.send_resp(200, body)
  end

  defp get_header(conn, key) when is_binary(key) do
    conn
    |> Plug.Conn.get_req_header(key)
    |> List.first()
  end

  defp read_body(conn, acc) do
    case Plug.Conn.read_body(conn) do
      {:ok, chunk, conn} ->
        {:ok, acc <> chunk, conn}

      {:more, chunk, conn} ->
        read_body(conn, acc <> chunk)
    end
  end

  defp get_object(store, bucket, key)
       when is_pid(store) and is_binary(bucket) and is_binary(key) do
    Agent.get(store, fn state ->
      state
      |> Map.get(bucket, %{})
      |> Map.get(key)
    end)
  end

  defp put_object(store, bucket, key, body, etag)
       when is_pid(store) and is_binary(bucket) and is_binary(key) and is_binary(body) and
              is_binary(etag) do
    Agent.update(store, fn state ->
      bucket_state = Map.get(state, bucket, %{})
      next_bucket = Map.put(bucket_state, key, %{body: body, etag: etag})
      Map.put(state, bucket, next_bucket)
    end)
  end

  defp list_keys(store, bucket) when is_pid(store) and is_binary(bucket) do
    Agent.get(store, fn state ->
      state
      |> Map.get(bucket, %{})
      |> Map.keys()
    end)
  end

  defp etag_for(body) when is_binary(body) do
    digest =
      :crypto.hash(:md5, body)
      |> Base.encode16(case: :lower)

    ~s("#{digest}")
  end

  defp xml_escape(value) when is_binary(value) do
    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
    |> String.replace("'", "&apos;")
  end
end
