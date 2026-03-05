defmodule Starcite.Session.RuntimeSnapshot do
  @moduledoc """
  Strict runtime snapshot schema used for freeze/hydrate session transitions.

  Snapshots are stored inside session metadata under `metadata_key/0`.
  """

  alias Starcite.Session

  @metadata_key "__starcite_runtime_v1"
  @schema_version 1

  @enforce_keys [:archived_seq, :tail_keep, :producer_max_entries]
  defstruct [:archived_seq, :tail_keep, :producer_max_entries]

  @type t :: %__MODULE__{
          archived_seq: non_neg_integer(),
          tail_keep: pos_integer(),
          producer_max_entries: pos_integer()
        }

  @spec metadata_key() :: String.t()
  def metadata_key, do: @metadata_key

  @spec from_session(Session.t()) :: t()
  def from_session(%Session{} = session) do
    %__MODULE__{
      archived_seq: session.archived_seq,
      tail_keep: session.retention.tail_keep,
      producer_max_entries: session.retention.producer_max_entries
    }
  end

  @spec put_in_metadata(map(), t()) :: map()
  def put_in_metadata(metadata, %__MODULE__{} = snapshot) when is_map(metadata) do
    Map.put(metadata, @metadata_key, encode(snapshot))
  end

  @spec drop_from_metadata(map()) :: map()
  def drop_from_metadata(metadata) when is_map(metadata) do
    Map.delete(metadata, @metadata_key)
  end

  @spec decode_from_metadata(map()) ::
          {:ok, t()}
          | {:error, :snapshot_missing | :invalid_snapshot | :unsupported_snapshot_version}
  def decode_from_metadata(metadata) when is_map(metadata) do
    case Map.get(metadata, @metadata_key) do
      %{} = payload ->
        decode(payload)

      nil ->
        {:error, :snapshot_missing}

      _other ->
        {:error, :invalid_snapshot}
    end
  end

  @spec encode(t()) :: map()
  def encode(%__MODULE__{} = snapshot) do
    %{
      "schema_version" => @schema_version,
      "archived_seq" => snapshot.archived_seq,
      "tail_keep" => snapshot.tail_keep,
      "producer_max_entries" => snapshot.producer_max_entries
    }
  end

  @spec decode(map()) :: {:ok, t()} | {:error, :invalid_snapshot | :unsupported_snapshot_version}
  def decode(%{"schema_version" => version})
      when is_integer(version) and version != @schema_version,
      do: {:error, :unsupported_snapshot_version}

  def decode(%{
        "schema_version" => @schema_version,
        "archived_seq" => archived_seq,
        "tail_keep" => tail_keep,
        "producer_max_entries" => producer_max_entries
      })
      when is_integer(archived_seq) and archived_seq >= 0 and is_integer(tail_keep) and
             tail_keep > 0 and is_integer(producer_max_entries) and producer_max_entries > 0 do
    {:ok,
     %__MODULE__{
       archived_seq: archived_seq,
       tail_keep: tail_keep,
       producer_max_entries: producer_max_entries
     }}
  end

  def decode(_payload), do: {:error, :invalid_snapshot}
end
