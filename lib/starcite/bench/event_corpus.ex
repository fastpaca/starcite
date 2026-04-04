defmodule Starcite.Bench.EventCorpus do
  @moduledoc """
  Builds sanitized benchmark event corpora from archived S3 event chunks.

  The benchmark corpus keeps request-shaping fields that matter for append
  workloads (`type`, `payload`, optional `source`, and optional `metadata`)
  while stripping archival-only fields and masking string content.
  """

  alias Starcite.Storage.EventArchive.S3.{Layout, Schema}

  @schema_version 1
  @default_object_limit 8
  @default_event_limit 128

  @type template :: %{
          required(:type) => String.t(),
          required(:payload) => map(),
          optional(:source) => String.t(),
          optional(:metadata) => map()
        }

  @type document :: %{
          required(:schema_version) => pos_integer(),
          required(:source) => String.t(),
          required(:generated_at) => String.t(),
          required(:events) => [template()],
          required(:sampled_object_count) => non_neg_integer(),
          required(:sampled_event_count) => non_neg_integer(),
          required(:type_counts) => %{optional(String.t()) => non_neg_integer()}
        }

  @spec sample_archive(map(), keyword()) ::
          {:ok, document()} | {:error, :archive_read_unavailable | :empty_archive}
  def sample_archive(config, opts \\ []) when is_map(config) and is_list(opts) do
    object_limit = positive_integer_opt!(opts, :object_limit, @default_object_limit)
    event_limit = positive_integer_opt!(opts, :event_limit, @default_event_limit)
    source = Keyword.get(opts, :source, "s3_archive_sample")
    sample_fun = Keyword.get(opts, :sample_fun, &Enum.take_random/2)

    with {:ok, keys} <- list_event_keys(config),
         false <- keys == [] do
      sampled_keys = sample_items(keys, object_limit, sample_fun)

      sampled_events =
        sampled_keys
        |> Enum.reduce_while({:ok, []}, fn key, {:ok, acc} ->
          case fetch_chunk_templates(config, key) do
            {:ok, templates} -> {:cont, {:ok, acc ++ templates}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)

      with {:ok, events} <- sampled_events,
           false <- events == [] do
        selected_events = sample_items(events, event_limit, sample_fun)

        {:ok,
         document(selected_events,
           source: source,
           sampled_object_count: length(sampled_keys),
           sampled_event_count: length(events)
         )}
      else
        true -> {:error, :empty_archive}
        {:error, _reason} = error -> error
      end
    else
      true -> {:error, :empty_archive}
      {:error, _reason} = error -> error
    end
  end

  @spec document([template()], keyword()) :: document()
  def document(events, opts \\ []) when is_list(events) and is_list(opts) do
    %{
      schema_version: @schema_version,
      source: Keyword.get(opts, :source, "generated"),
      generated_at: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601(),
      sampled_object_count: Keyword.get(opts, :sampled_object_count, 0),
      sampled_event_count: Keyword.get(opts, :sampled_event_count, length(events)),
      type_counts: count_types(events),
      events: events
    }
  end

  @spec sanitize_event_template(map()) :: template()
  def sanitize_event_template(event) when is_map(event) do
    type = fetch_required_string!(event, :type)
    payload = fetch_required_map!(event, :payload)

    %{
      type: type,
      payload: sanitize_value(payload)
    }
    |> maybe_put_sanitized_string(:source, fetch_optional_string(event, :source))
    |> maybe_put_sanitized_map(:metadata, sanitize_metadata(event))
  end

  @spec write_document!(document(), Path.t()) :: :ok
  def write_document!(document, path) when is_map(document) and is_binary(path) do
    path
    |> Path.dirname()
    |> File.mkdir_p!()

    path
    |> File.write!(Jason.encode_to_iodata!(document, pretty: true))

    :ok
  end

  defp list_event_keys(config) when is_map(config) do
    case client(config).list_keys(config, Layout.event_prefix(config)) do
      {:ok, keys} ->
        {:ok, Enum.filter(keys, &String.ends_with?(&1, ".ndjson"))}

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp fetch_chunk_templates(config, key) when is_map(config) and is_binary(key) do
    case client(config).get_object(config, key) do
      {:ok, {body, _etag}} ->
        case Schema.decode_event_chunk(body, nil) do
          {:ok, events, _migration_required} ->
            {:ok, Enum.map(events, &sanitize_event_template/1)}

          {:error, :archive_read_unavailable} ->
            {:error, :archive_read_unavailable}
        end

      {:ok, :not_found} ->
        {:ok, []}

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp sanitize_metadata(event) when is_map(event) do
    case fetch_optional_map(event, :metadata) do
      nil ->
        nil

      metadata ->
        sanitized =
          metadata
          |> Enum.reject(fn {key, _value} -> system_metadata_key?(key) end)
          |> Map.new(fn {key, value} -> {key, sanitize_value(value)} end)

        if map_size(sanitized) == 0, do: nil, else: sanitized
    end
  end

  defp sanitize_value(value) when is_binary(value) do
    String.duplicate("x", byte_size(value))
  end

  defp sanitize_value(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {key, sanitize_value(nested)} end)
  end

  defp sanitize_value(value) when is_list(value) do
    Enum.map(value, &sanitize_value/1)
  end

  defp sanitize_value(value), do: value

  defp maybe_put_sanitized_string(template, _key, nil) when is_map(template), do: template

  defp maybe_put_sanitized_string(template, key, value)
       when is_map(template) and is_atom(key) and is_binary(value) do
    Map.put(template, key, sanitize_value(value))
  end

  defp maybe_put_sanitized_map(template, _key, nil) when is_map(template), do: template

  defp maybe_put_sanitized_map(template, key, value)
       when is_map(template) and is_atom(key) and is_map(value) do
    Map.put(template, key, value)
  end

  defp fetch_required_string!(map, key) when is_map(map) and is_atom(key) do
    case fetch_value(map, key) do
      value when is_binary(value) and value != "" ->
        value

      value ->
        raise ArgumentError,
              "invalid event corpus field #{inspect(key)}: #{inspect(value)} (expected non-empty string)"
    end
  end

  defp fetch_required_map!(map, key) when is_map(map) and is_atom(key) do
    case fetch_value(map, key) do
      value when is_map(value) ->
        value

      value ->
        raise ArgumentError,
              "invalid event corpus field #{inspect(key)}: #{inspect(value)} (expected map)"
    end
  end

  defp fetch_optional_string(map, key) when is_map(map) and is_atom(key) do
    case fetch_value(map, key) do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  defp fetch_optional_map(map, key) when is_map(map) and is_atom(key) do
    case fetch_value(map, key) do
      value when is_map(value) -> value
      _ -> nil
    end
  end

  defp fetch_value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        Map.get(map, Atom.to_string(key))
    end
  end

  defp system_metadata_key?(key) when is_atom(key) do
    key
    |> Atom.to_string()
    |> system_metadata_key?()
  end

  defp system_metadata_key?(key) when is_binary(key) do
    String.starts_with?(key, "starcite_")
  end

  defp system_metadata_key?(_key), do: false

  defp sample_items(items, limit, _sample_fun)
       when is_list(items) and is_integer(limit) and limit >= length(items) do
    items
  end

  defp sample_items(items, limit, sample_fun)
       when is_list(items) and is_integer(limit) and limit > 0 and is_function(sample_fun, 2) do
    sample_fun.(items, limit)
  end

  defp count_types(events) when is_list(events) do
    Enum.reduce(events, %{}, fn
      %{type: type}, acc when is_binary(type) and type != "" ->
        Map.update(acc, type, 1, &(&1 + 1))

      _event, acc ->
        acc
    end)
  end

  defp positive_integer_opt!(opts, key, default)
       when is_list(opts) and is_atom(key) and is_integer(default) and default > 0 do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "invalid #{inspect(key)} for benchmark corpus: #{inspect(value)} (expected positive integer)"
    end
  end

  defp client(%{client_mod: client_mod}), do: client_mod
end
