defmodule Starcite.TestSupport.EventArchiveClient do
  @moduledoc false

  alias Starcite.Storage.EventArchive.S3.Schema

  @objects_table :starcite_test_event_archive_objects
  @logs_table :starcite_test_event_archive_logs
  @state_table :starcite_test_event_archive_state

  def reset do
    ensure_tables()
    :ets.delete_all_objects(@objects_table)
    :ets.delete_all_objects(@logs_table)
    :ets.delete_all_objects(@state_table)
    :ets.insert(@state_table, [{:read_mode, :ok}, {:write_mode, :ok}, {:log_seq, 0}])
    :ok
  end

  def set_read_mode(mode) when mode in [:ok, :unavailable] do
    ensure_tables()
    :ets.insert(@state_table, {:read_mode, mode})
    :ok
  end

  def set_write_mode(mode) when mode in [:ok, :unavailable] do
    ensure_tables()
    :ets.insert(@state_table, {:write_mode, mode})
    :ok
  end

  def clear_logs do
    ensure_tables()
    :ets.delete_all_objects(@logs_table)
    :ets.insert(@state_table, {:log_seq, 0})
    :ok
  end

  def get_object(%{bucket: bucket}, key) when is_binary(bucket) and is_binary(key) do
    ensure_tables()
    log(:get, key)

    case mode(:read_mode) do
      :unavailable ->
        {:error, :unavailable}

      :ok ->
        case :ets.lookup(@objects_table, {bucket, key}) do
          [{{^bucket, ^key}, body, etag}] -> {:ok, {body, etag}}
          [] -> {:ok, :not_found}
        end
    end
  end

  def put_object(%{bucket: bucket}, key, body, opts \\ [])
      when is_binary(bucket) and is_binary(key) and is_binary(body) and is_list(opts) do
    ensure_tables()
    log(:put, key, body)

    case mode(:write_mode) do
      :unavailable ->
        {:error, :unavailable}

      :ok ->
        existing =
          case :ets.lookup(@objects_table, {bucket, key}) do
            [{{^bucket, ^key}, existing_body, existing_etag}] ->
              %{body: existing_body, etag: existing_etag}

            [] ->
              nil
          end

        if precondition_failed?(existing, opts) do
          {:error, :precondition_failed}
        else
          etag = etag_for(body)
          true = :ets.insert(@objects_table, {{bucket, key}, body, etag})
          :ok
        end
    end
  end

  def list_keys(%{bucket: bucket}, prefix) when is_binary(bucket) and is_binary(prefix) do
    ensure_tables()

    keys =
      @objects_table
      |> :ets.tab2list()
      |> Enum.flat_map(fn
        {{^bucket, key}, _body, _etag} -> [key]
        _other -> []
      end)
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.sort()

    {:ok, keys}
  end

  def call_count(op, key) when op in [:get, :put] and is_binary(key) do
    ensure_tables()

    @logs_table
    |> :ets.tab2list()
    |> Enum.count(fn {_seq, logged_op, logged_key, _body} ->
      logged_op == op and logged_key == key
    end)
  end

  def read_keys do
    ensure_tables()

    @logs_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.flat_map(fn
      {_seq, :get, key, _body} -> [key]
      _other -> []
    end)
  end

  def write_batches do
    ensure_tables()

    @logs_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce({[], MapSet.new()}, fn
      {_seq, :put, key, body}, {batches, seen_rows} ->
        {new_rows, next_seen_rows} =
          key
          |> decode_write_batch(body)
          |> Enum.reduce({[], seen_rows}, fn row, {acc, seen} ->
            row_key = {row.session_id, row.seq}

            if MapSet.member?(seen, row_key) do
              {acc, seen}
            else
              {[row | acc], MapSet.put(seen, row_key)}
            end
          end)

        batches =
          case Enum.reverse(new_rows) do
            [] -> batches
            batch -> batches ++ [batch]
          end

        {batches, next_seen_rows}

      _other, acc ->
        acc
    end)
    |> elem(0)
  end

  def writes do
    write_batches()
    |> List.flatten()
    |> Enum.uniq_by(fn row -> {row.session_id, row.seq} end)
  end

  defp decode_write_batch(key, body) when is_binary(key) and is_binary(body) do
    {tenant_id, session_id} = event_identifiers_from_key!(key)
    {:ok, events, _migration_required} = Schema.decode_event_chunk(body, tenant_id)

    Enum.map(events, fn event ->
      event
      |> Map.put(:session_id, session_id)
      |> Map.put(:tenant_id, tenant_id)
    end)
  end

  defp event_identifiers_from_key!(key) when is_binary(key) do
    case key |> String.split("/") |> Enum.take(-5) do
      ["events", "v1", tenant_segment, session_segment, _chunk_file] ->
        {
          Base.url_decode64!(tenant_segment, padding: false),
          Base.url_decode64!(session_segment, padding: false)
        }

      _other ->
        raise ArgumentError, "unexpected event archive key: #{inspect(key)}"
    end
  end

  defp mode(key) do
    case :ets.lookup(@state_table, key) do
      [{^key, value}] -> value
      [] -> :ok
    end
  end

  defp log(op, key, body \\ nil) when op in [:get, :put] and is_binary(key) do
    seq = :ets.update_counter(@state_table, :log_seq, {2, 1}, {:log_seq, 0})
    true = :ets.insert(@logs_table, {seq, op, key, body})
    :ok
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

  defp ensure_tables do
    ensure_table(@objects_table, [:named_table, :public, :set])
    ensure_table(@logs_table, [:named_table, :public, :ordered_set])
    ensure_table(@state_table, [:named_table, :public, :set])

    if :ets.lookup(@state_table, :log_seq) == [] do
      true = :ets.insert(@state_table, {:log_seq, 0})
    end

    if :ets.lookup(@state_table, :read_mode) == [] do
      true = :ets.insert(@state_table, {:read_mode, :ok})
    end

    if :ets.lookup(@state_table, :write_mode) == [] do
      true = :ets.insert(@state_table, {:write_mode, :ok})
    end

    :ok
  end

  defp ensure_table(name, options) do
    case :ets.info(name) do
      :undefined -> :ets.new(name, options)
      _info -> name
    end
  end
end
