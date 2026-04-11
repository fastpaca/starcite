defmodule Starcite.Session.Projections do
  @moduledoc """
  Projection state carried as part of a session.

  Projection items are first-class session state. Raw events remain the source
  of truth, while the active latest projection items become part of the default
  composed replay view.
  """

  alias __MODULE__, as: Projections

  @enforce_keys [:items]
  defstruct items: %{}

  @type item :: %{
          required(:item_id) => String.t(),
          required(:version) => pos_integer(),
          required(:from_seq) => pos_integer(),
          required(:to_seq) => pos_integer(),
          required(:payload) => map(),
          required(:metadata) => map(),
          optional(:schema) => String.t() | nil,
          optional(:content_type) => String.t() | nil,
          required(:inserted_at) => String.t()
        }

  @type item_entry :: %{
          required(:latest_version) => pos_integer(),
          required(:versions) => [item()]
        }

  @type t :: %Projections{
          items: %{optional(String.t()) => item_entry()}
        }

  @spec new() :: t()
  def new, do: %Projections{items: %{}}

  @spec from_map(map() | nil) :: {:ok, t()} | {:error, :invalid_projection_item}
  def from_map(nil), do: {:ok, new()}
  def from_map(%{} = map), do: normalize_state(map)
  def from_map(_other), do: {:error, :invalid_projection_item}

  @spec to_map(t()) :: map()
  def to_map(%Projections{items: items}) when is_map(items) do
    %{
      "items" =>
        Map.new(items, fn {item_id, %{latest_version: latest_version, versions: versions}} ->
          {item_id,
           %{
             "latest_version" => latest_version,
             "versions" => Enum.map(versions, &item_to_storage/1)
           }}
        end)
    }
  end

  @spec latest_items(t()) :: [item()]
  def latest_items(%Projections{items: items}) when is_map(items) do
    items
    |> Enum.map(fn {_item_id, %{latest_version: latest_version, versions: versions}} ->
      version_item!(versions, latest_version)
    end)
    |> Enum.sort_by(fn %{from_seq: from_seq, to_seq: to_seq} -> {from_seq, to_seq} end)
  end

  @spec mirrorable?(t(), non_neg_integer()) :: boolean()
  def mirrorable?(%Projections{} = state, max_seq)
      when is_integer(max_seq) and max_seq >= 0 do
    Enum.all?(latest_items(state), fn %{to_seq: to_seq} ->
      is_integer(to_seq) and to_seq <= max_seq
    end)
  end

  @spec get_item(t(), String.t()) :: {:ok, item()} | {:error, :projection_item_not_found}
  def get_item(%Projections{items: items}, item_id)
      when is_map(items) and is_binary(item_id) and item_id != "" do
    case Map.get(items, item_id) do
      %{latest_version: latest_version, versions: versions} ->
        {:ok, version_item!(versions, latest_version)}

      nil ->
        {:error, :projection_item_not_found}
    end
  end

  def get_item(%Projections{}, _item_id), do: {:error, :projection_item_not_found}

  @spec item_versions(t(), String.t()) :: {:ok, [item()]} | {:error, :projection_item_not_found}
  def item_versions(%Projections{items: items}, item_id)
      when is_map(items) and is_binary(item_id) and item_id != "" do
    case Map.get(items, item_id) do
      %{versions: versions} when is_list(versions) and versions != [] ->
        {:ok, versions}

      _other ->
        {:error, :projection_item_not_found}
    end
  end

  def item_versions(%Projections{}, _item_id), do: {:error, :projection_item_not_found}

  @spec get_item_version(t(), String.t(), pos_integer()) ::
          {:ok, item()}
          | {:error, :projection_item_not_found | :projection_item_version_not_found}
  def get_item_version(%Projections{} = state, item_id, version)
      when is_binary(item_id) and item_id != "" and is_integer(version) and version > 0 do
    with {:ok, versions} <- item_versions(state, item_id),
         {:ok, item} <- version_item(versions, version) do
      {:ok, item}
    end
  end

  def get_item_version(%Projections{}, _item_id, _version),
    do: {:error, :projection_item_not_found}

  @spec put_items(t(), non_neg_integer(), [map()]) ::
          {:ok, t(), [item()]}
          | {:error,
             :invalid_projection_item
             | :projection_item_duplicate
             | :projection_item_overlap
             | :projection_item_exists}
  def put_items(%Projections{} = state, session_last_seq, items)
      when is_integer(session_last_seq) and session_last_seq >= 0 and is_list(items) and
             items != [] do
    inserted_at = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    with :ok <- ensure_unique_batch_item_ids(items),
         {:ok, updates, stored_items} <-
           prepare_updates(state, session_last_seq, items, inserted_at),
         {:ok, next_state} <- apply_updates(state, updates),
         :ok <- ensure_latest_overlap_free(next_state) do
      {:ok, next_state, stored_items}
    end
  end

  def put_items(%Projections{}, _session_last_seq, _items), do: {:error, :invalid_projection_item}

  @spec delete_item(t(), String.t()) :: {:ok, t()} | {:error, :projection_item_not_found}
  def delete_item(%Projections{items: items}, item_id)
      when is_map(items) and is_binary(item_id) and item_id != "" do
    if Map.has_key?(items, item_id) do
      {:ok, %Projections{items: Map.delete(items, item_id)}}
    else
      {:error, :projection_item_not_found}
    end
  end

  def delete_item(%Projections{}, _item_id), do: {:error, :projection_item_not_found}

  defp normalize_state(%{"items" => items}) when is_map(items), do: normalize_items(items)
  defp normalize_state(%{items: items}) when is_map(items), do: normalize_items(items)
  defp normalize_state(%{}), do: {:ok, new()}

  defp normalize_items(items) when is_map(items) do
    items
    |> Enum.reduce_while({:ok, %{}}, fn {item_id, entry}, {:ok, acc} ->
      with {:ok, item_id} <- required_non_empty_string(item_id),
           {:ok, normalized_entry} <- normalize_entry(entry) do
        {:cont, {:ok, Map.put(acc, item_id, normalized_entry)}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized_items} ->
        state = %Projections{items: normalized_items}

        case ensure_latest_overlap_free(state) do
          :ok -> {:ok, state}
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_entry(%{"latest_version" => latest_version, "versions" => versions}),
    do: normalize_entry(%{latest_version: latest_version, versions: versions})

  defp normalize_entry(%{latest_version: latest_version, versions: versions})
       when is_integer(latest_version) and latest_version > 0 and is_list(versions) do
    with {:ok, normalized_versions} <- normalize_versions(versions),
         :ok <- ensure_latest_version_present(normalized_versions, latest_version) do
      {:ok, %{latest_version: latest_version, versions: normalized_versions}}
    end
  end

  defp normalize_entry(_entry), do: {:error, :invalid_projection_item}

  defp normalize_versions(versions) when is_list(versions) do
    versions
    |> Enum.reduce_while({:ok, []}, fn version_item, {:ok, acc} ->
      case normalize_item(version_item) do
        {:ok, item} -> {:cont, {:ok, [item | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized_versions} ->
        sorted_versions =
          normalized_versions
          |> Enum.reverse()
          |> Enum.sort_by(& &1.version)

        if duplicate_versions?(sorted_versions) do
          {:error, :invalid_projection_item}
        else
          {:ok, sorted_versions}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp ensure_latest_version_present(versions, latest_version)
       when is_list(versions) and is_integer(latest_version) and latest_version > 0 do
    if Enum.any?(versions, &(&1.version == latest_version)) do
      :ok
    else
      {:error, :invalid_projection_item}
    end
  end

  defp prepare_updates(%Projections{items: current_items}, session_last_seq, items, inserted_at)
       when is_map(current_items) and is_integer(session_last_seq) and session_last_seq >= 0 and
              is_list(items) and is_binary(inserted_at) do
    items
    |> Enum.reduce_while({:ok, %{}, []}, fn raw_item, {:ok, updates, stored_items} ->
      with {:ok, normalized_item} <- normalize_item(raw_item, inserted_at),
           :ok <- ensure_item_within_session(normalized_item, session_last_seq),
           {:ok, next_entry, stored_item} <-
             merge_item(
               Map.get(
                 updates,
                 normalized_item.item_id,
                 Map.get(current_items, normalized_item.item_id)
               ),
               normalized_item
             ) do
        {:cont,
         {:ok, Map.put(updates, normalized_item.item_id, next_entry),
          [stored_item | stored_items]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, updates, stored_items} -> {:ok, updates, Enum.reverse(stored_items)}
      {:error, _reason} = error -> error
    end
  end

  defp apply_updates(%Projections{items: current_items}, updates)
       when is_map(current_items) and is_map(updates) do
    {:ok, %Projections{items: Map.merge(current_items, updates)}}
  end

  defp merge_item(nil, %{} = item) do
    {:ok, %{latest_version: item.version, versions: [item]}, item}
  end

  defp merge_item(%{latest_version: latest_version, versions: versions}, %{} = item)
       when is_integer(latest_version) and latest_version > 0 and is_list(versions) do
    case version_item(versions, item.version) do
      {:ok, existing_item} ->
        if comparable_item(existing_item) == comparable_item(item) do
          {:ok, %{latest_version: latest_version, versions: versions}, existing_item}
        else
          {:error, :projection_item_exists}
        end

      {:error, :projection_item_version_not_found} ->
        next_latest_version = max(latest_version, item.version)

        next_versions =
          versions
          |> Kernel.++([item])
          |> Enum.sort_by(& &1.version)

        {:ok, %{latest_version: next_latest_version, versions: next_versions}, item}
    end
  end

  defp ensure_unique_batch_item_ids(items) when is_list(items) do
    item_ids =
      Enum.map(items, fn
        %{item_id: item_id} -> item_id
        %{"item_id" => item_id} -> item_id
        _other -> nil
      end)

    if length(Enum.reject(item_ids, &is_nil/1)) == length(Enum.uniq(item_ids)) do
      :ok
    else
      {:error, :projection_item_duplicate}
    end
  end

  defp ensure_item_within_session(%{from_seq: from_seq, to_seq: to_seq}, session_last_seq)
       when is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq and
              is_integer(session_last_seq) and session_last_seq >= 0 do
    if to_seq <= session_last_seq do
      :ok
    else
      {:error, :invalid_projection_item}
    end
  end

  defp ensure_latest_overlap_free(%Projections{} = state) do
    state
    |> latest_items()
    |> Enum.reduce_while(nil, fn item, previous ->
      cond do
        previous == nil ->
          {:cont, item}

        ranges_overlap?(previous.from_seq, previous.to_seq, item.from_seq, item.to_seq) ->
          {:halt, {:error, :projection_item_overlap}}

        true ->
          {:cont, item}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      _ -> :ok
    end
  end

  defp normalize_item(raw_item, inserted_at \\ nil)

  defp normalize_item(
         %{
           item_id: item_id,
           version: version,
           from_seq: from_seq,
           to_seq: to_seq,
           payload: payload,
           metadata: metadata
         } = raw_item,
         inserted_at
       ) do
    normalize_item_map(
      raw_item,
      item_id,
      version,
      from_seq,
      to_seq,
      payload,
      metadata,
      inserted_at
    )
  end

  defp normalize_item(
         %{
           "item_id" => item_id,
           "version" => version,
           "from_seq" => from_seq,
           "to_seq" => to_seq,
           "payload" => payload,
           "metadata" => metadata
         } = raw_item,
         inserted_at
       ) do
    normalize_item_map(
      raw_item,
      item_id,
      version,
      from_seq,
      to_seq,
      payload,
      metadata,
      inserted_at
    )
  end

  defp normalize_item(_raw_item, _inserted_at), do: {:error, :invalid_projection_item}

  defp normalize_item_map(
         raw_item,
         item_id,
         version,
         from_seq,
         to_seq,
         payload,
         metadata,
         inserted_at
       )
       when is_map(raw_item) do
    with {:ok, item_id} <- required_non_empty_string(item_id),
         :ok <- ensure_positive_integer(version),
         :ok <- ensure_positive_integer(from_seq),
         :ok <- ensure_positive_integer(to_seq),
         true <- to_seq >= from_seq,
         true <- is_map(payload),
         true <- is_map(metadata),
         {:ok, inserted_at} <- normalize_inserted_at(raw_item, inserted_at),
         {:ok, schema} <- optional_string(raw_item, :schema, "schema"),
         {:ok, content_type} <- optional_string(raw_item, :content_type, "content_type") do
      {:ok,
       %{
         item_id: item_id,
         version: version,
         from_seq: from_seq,
         to_seq: to_seq,
         payload: payload,
         metadata: metadata,
         schema: schema,
         content_type: content_type,
         inserted_at: inserted_at
       }}
    else
      false -> {:error, :invalid_projection_item}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_inserted_at(raw_item, nil) when is_map(raw_item) do
    inserted_at =
      Map.get(raw_item, :inserted_at) ||
        Map.get(raw_item, "inserted_at")

    required_non_empty_string(inserted_at)
  end

  defp normalize_inserted_at(_raw_item, inserted_at), do: required_non_empty_string(inserted_at)

  defp optional_string(raw_item, atom_key, string_key) when is_map(raw_item) do
    value = Map.get(raw_item, atom_key) || Map.get(raw_item, string_key)

    cond do
      is_nil(value) ->
        {:ok, nil}

      is_binary(value) and value != "" ->
        {:ok, value}

      true ->
        {:error, :invalid_projection_item}
    end
  end

  defp required_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_non_empty_string(_value), do: {:error, :invalid_projection_item}

  defp ensure_positive_integer(value) when is_integer(value) and value > 0, do: :ok
  defp ensure_positive_integer(_value), do: {:error, :invalid_projection_item}

  defp duplicate_versions?(versions) when is_list(versions) do
    version_numbers = Enum.map(versions, & &1.version)
    length(version_numbers) != length(Enum.uniq(version_numbers))
  end

  defp version_item(versions, version)
       when is_list(versions) and is_integer(version) and version > 0 do
    case Enum.find(versions, &(&1.version == version)) do
      nil -> {:error, :projection_item_version_not_found}
      item -> {:ok, item}
    end
  end

  defp version_item!(versions, version) do
    {:ok, item} = version_item(versions, version)
    item
  end

  defp comparable_item(item) when is_map(item) do
    %{
      item_id: Map.get(item, :item_id),
      version: Map.get(item, :version),
      from_seq: Map.get(item, :from_seq),
      to_seq: Map.get(item, :to_seq),
      schema: Map.get(item, :schema),
      content_type: Map.get(item, :content_type),
      payload: Map.get(item, :payload),
      metadata: Map.get(item, :metadata)
    }
  end

  defp item_to_storage(item) when is_map(item) do
    %{
      "item_id" => item.item_id,
      "version" => item.version,
      "from_seq" => item.from_seq,
      "to_seq" => item.to_seq,
      "schema" => item.schema,
      "content_type" => item.content_type,
      "payload" => item.payload,
      "metadata" => item.metadata,
      "inserted_at" => item.inserted_at
    }
  end

  defp ranges_overlap?(from_seq, to_seq, existing_from, existing_to)
       when is_integer(from_seq) and is_integer(to_seq) and is_integer(existing_from) and
              is_integer(existing_to) do
    not (to_seq < existing_from or existing_to < from_seq)
  end
end
