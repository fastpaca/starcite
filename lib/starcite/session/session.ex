defmodule Starcite.Session do
  @moduledoc """
  Lean session snapshot used across the read path.
  """

  alias __MODULE__, as: Session
  alias Starcite.Session.Header

  @enforce_keys [:id, :tenant_id, :last_seq, :archived_seq]
  defstruct [:id, :tenant_id, :last_seq, :archived_seq]

  @type t :: %Session{
          id: String.t(),
          tenant_id: String.t(),
          last_seq: non_neg_integer(),
          archived_seq: non_neg_integer()
        }

  @default_tail_keep 1_000
  @default_producer_max_entries 10_000
  @retention_defaults_cache_key {__MODULE__, :retention_defaults}

  @doc """
  Create a new session.
  """
  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) when is_binary(id) and is_list(opts) do
    reject_retention_overrides!(opts)
    # Create-time metadata lives in Header; the hot session stays lean.
    header = Header.new(id, opts)
    build_session(header.id, header.tenant_id)
  end

  @doc false
  @spec new_raft(String.t(), String.t()) :: t()
  def new_raft(id, tenant_id)
      when is_binary(id) and id != "" and is_binary(tenant_id) and tenant_id != "" do
    build_session(id, tenant_id)
  end

  @doc false
  @spec new_from_header(Header.t()) :: t()
  def new_from_header(%Header{id: id, tenant_id: tenant_id}) do
    build_session(id, tenant_id)
  end

  @doc false
  @spec hydrate(Header.t(), non_neg_integer()) :: t()
  def hydrate(%Header{} = header, archived_seq)
      when is_integer(archived_seq) and archived_seq >= 0 do
    %Session{new_from_header(header) | last_seq: archived_seq, archived_seq: archived_seq}
  end

  @spec tail_size(t()) :: non_neg_integer()
  def tail_size(%Session{archived_seq: archived_seq, last_seq: last_seq}) do
    max(last_seq - archived_seq, 0)
  end

  @spec tail_keep() :: pos_integer()
  def tail_keep do
    retention_defaults().tail_keep
  end

  @spec producer_max_entries() :: pos_integer()
  def producer_max_entries do
    retention_defaults().producer_max_entries
  end

  @doc false
  @spec retention_defaults() :: %{tail_keep: pos_integer(), producer_max_entries: pos_integer()}
  def retention_defaults do
    raw = {default_tail_keep(), default_producer_max_entries()}

    case :persistent_term.get(@retention_defaults_cache_key, :undefined) do
      {^raw, defaults} ->
        defaults

      _ ->
        defaults = %{tail_keep: elem(raw, 0), producer_max_entries: elem(raw, 1)}
        :persistent_term.put(@retention_defaults_cache_key, {raw, defaults})
        defaults
    end
  end

  @spec to_map(t(), Header.t()) :: map()
  def to_map(%Session{} = session, %Header{} = header),
    do: Header.to_map(header, session.last_seq)

  defp build_session(id, tenant_id)
       when is_binary(id) and id != "" and is_binary(tenant_id) and tenant_id != "" do
    %Session{
      id: id,
      tenant_id: tenant_id,
      last_seq: 0,
      archived_seq: 0
    }
  end

  defp default_tail_keep do
    Application.get_env(:starcite, :tail_keep, @default_tail_keep)
  end

  defp default_producer_max_entries do
    Application.get_env(:starcite, :producer_max_entries, @default_producer_max_entries)
  end

  defp reject_retention_overrides!(opts) when is_list(opts) do
    if Keyword.has_key?(opts, :tail_keep) do
      raise ArgumentError, "per-session tail_keep is no longer supported"
    end

    if Keyword.has_key?(opts, :producer_max_entries) do
      raise ArgumentError, "per-session producer_max_entries is no longer supported"
    end

    :ok
  end
end
