defmodule Starcite.Session do
  @moduledoc """
  Session aggregate root for replicated runtime state.
  """

  alias __MODULE__, as: Session
  alias Starcite.Session.{Events, Header, ProducerIndex, Projections}

  @type event_input :: Events.event_input()
  @type event :: Events.event()

  @enforce_keys [
    :id,
    :tenant_id,
    :epoch,
    :last_seq,
    :archived_seq,
    :projection_version,
    :projections
  ]
  defstruct [:id, :tenant_id, :epoch, :last_seq, :archived_seq, :projection_version, :projections]

  @type t :: %Session{
          id: String.t(),
          tenant_id: String.t(),
          epoch: non_neg_integer(),
          last_seq: non_neg_integer(),
          archived_seq: non_neg_integer(),
          projection_version: non_neg_integer(),
          projections: Projections.t()
        }

  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) when is_binary(id) and is_list(opts) do
    id
    |> Header.new(opts)
    |> new_from_header()
  end

  @spec new_from_header(Header.t()) :: t()
  def new_from_header(%Header{id: id, tenant_id: tenant_id}) do
    %Session{
      id: id,
      tenant_id: tenant_id,
      epoch: 0,
      last_seq: 0,
      archived_seq: 0,
      projection_version: 0,
      projections: Projections.new()
    }
  end

  @spec hydrate(Header.t(), non_neg_integer(), map() | nil, non_neg_integer()) ::
          {:ok, t()} | {:error, :invalid_projection_item}
  def hydrate(%Header{} = header, archived_seq, projection_state \\ nil, projection_version \\ 0)
      when is_integer(archived_seq) and archived_seq >= 0 and is_integer(projection_version) and
             projection_version >= 0 do
    with {:ok, projections} <- Projections.from_map(projection_state) do
      {effective_projections, effective_projection_version} =
        if Projections.mirrorable?(projections, archived_seq) do
          {projections, projection_version}
        else
          # The catalog is only a cold mirror for archived-safe projection state.
          # If the stored projection view extends past the archived watermark, drop
          # it on hydrate rather than rebuilding an impossible composed session.
          {Projections.new(), 0}
        end

      {:ok,
       %Session{
         new_from_header(header)
         | last_seq: archived_seq,
           archived_seq: archived_seq,
           projection_version: effective_projection_version,
           projections: effective_projections
       }}
    end
  end

  @spec put_projection_items(t(), [map()]) ::
          {:ok, t(), [Projections.item()]}
          | {:error,
             :invalid_projection_item
             | :projection_item_duplicate
             | :projection_item_overlap
             | :projection_item_exists}
  def put_projection_items(%Session{projections: %Projections{} = projections} = session, items)
      when is_list(items) do
    with {:ok, next_projections, stored_items} <-
           Projections.put_items(projections, session.last_seq, items) do
      {:ok,
       %Session{
         session
         | projections: next_projections,
           projection_version: session.projection_version + 1
       }, stored_items}
    end
  end

  def put_projection_items(%Session{}, _items), do: {:error, :invalid_projection_item}

  @spec delete_projection_item(t(), String.t()) ::
          {:ok, t()} | {:error, :projection_item_not_found}
  def delete_projection_item(
        %Session{projections: %Projections{} = projections} = session,
        item_id
      )
      when is_binary(item_id) and item_id != "" do
    with {:ok, next_projections} <- Projections.delete_item(projections, item_id) do
      {:ok,
       %Session{
         session
         | projections: next_projections,
           projection_version: session.projection_version + 1
       }}
    end
  end

  def delete_projection_item(%Session{}, _item_id), do: {:error, :projection_item_not_found}

  @spec latest_projection_items(t()) :: [Projections.item()]
  def latest_projection_items(%Session{projections: %Projections{} = projections}) do
    Projections.latest_items(projections)
  end

  @spec append_event(t(), map(), event_input()) ::
          {:appended, t(), ProducerIndex.t(), event()}
          | {:deduped, t(), ProducerIndex.t(), non_neg_integer()}
          | {:error, :producer_replay_conflict}
          | {:error, {:producer_seq_conflict, String.t(), pos_integer(), pos_integer()}}
          | {:error, :invalid_event}
  defdelegate append_event(session, producer_cursors, input), to: Events

  @spec persist_ack(t(), non_neg_integer()) :: {t(), non_neg_integer()}
  defdelegate persist_ack(session, upto_seq), to: Events

  @spec persist_ack(t(), non_neg_integer(), pos_integer()) :: {t(), non_neg_integer()}
  defdelegate persist_ack(session, upto_seq, tail_keep), to: Events

  @doc """
  Return the virtual tail size implied by retention metadata.
  """
  @spec tail_size(t()) :: non_neg_integer()
  defdelegate tail_size(session), to: Events

  @spec tail_keep() :: pos_integer()
  defdelegate tail_keep(), to: Events

  @spec producer_max_entries() :: pos_integer()
  defdelegate producer_max_entries(), to: Events

  @spec retention_defaults() :: %{tail_keep: pos_integer(), producer_max_entries: pos_integer()}
  defdelegate retention_defaults(), to: Events

  @spec normalize_epoch(t()) :: t()
  defdelegate normalize_epoch(session), to: Events

  @spec normalize_epoch_value(term()) :: non_neg_integer()
  defdelegate normalize_epoch_value(epoch), to: Events

  @spec to_map(t(), Header.t()) :: map()
  def to_map(%Session{} = session, %Header{} = header),
    do: Header.to_map(header, session.last_seq)
end
