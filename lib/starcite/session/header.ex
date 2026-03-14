defmodule Starcite.Session.Header do
  @moduledoc """
  Stable session header used for identity and catalog metadata.
  """

  alias __MODULE__, as: Header
  alias Starcite.Auth.Principal

  @enforce_keys [:id, :tenant_id, :created_at]
  # Header fields are persisted and rendered, but they do not belong in Raft's
  # append-time write state.
  defstruct [:id, :tenant_id, :title, :creator_principal, :metadata, :created_at]

  @type t :: %Header{
          id: String.t(),
          tenant_id: String.t(),
          title: String.t() | nil,
          creator_principal: Principal.t() | nil,
          metadata: map(),
          created_at: NaiveDateTime.t()
        }

  @doc """
  Build a session header from validated create inputs.
  """
  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) when is_binary(id) do
    created_at = Keyword.get(opts, :timestamp, NaiveDateTime.utc_now())

    creator_principal =
      optional_principal!(Keyword.get(opts, :creator_principal), :creator_principal)

    tenant_id = resolve_tenant_id!(Keyword.get(opts, :tenant_id), creator_principal)

    build_header(
      id,
      tenant_id,
      Keyword.get(opts, :title),
      creator_principal,
      Keyword.get(opts, :metadata, %{}),
      created_at
    )
  end

  @doc false
  @spec new_raft(String.t(), String.t() | nil, Principal.t() | nil, String.t(), map()) :: t()
  def new_raft(id, title, creator_principal, tenant_id, metadata)
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             (is_struct(creator_principal, Principal) or is_nil(creator_principal)) and
             is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    build_header(id, tenant_id, title, creator_principal, metadata, NaiveDateTime.utc_now())
  end

  @spec to_map(t(), non_neg_integer()) :: map()
  def to_map(%Header{} = header, last_seq)
      when is_integer(last_seq) and last_seq >= 0 do
    created_at = iso8601_utc(header.created_at)

    %{
      id: header.id,
      title: header.title,
      creator_principal: header.creator_principal,
      metadata: header.metadata,
      last_seq: last_seq,
      created_at: created_at,
      updated_at: created_at
    }
  end

  defp build_header(id, tenant_id, title, creator_principal, metadata, created_at)
       when is_binary(id) and id != "" and is_binary(tenant_id) and tenant_id != "" and
              (is_binary(title) or is_nil(title)) and
              (is_struct(creator_principal, Principal) or is_nil(creator_principal)) and
              is_map(metadata) and is_struct(created_at, NaiveDateTime) do
    %Header{
      id: id,
      tenant_id: tenant_id,
      title: title,
      creator_principal: creator_principal,
      metadata: metadata,
      created_at: created_at
    }
  end

  defp iso8601_utc(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp optional_principal!(nil, _field), do: nil
  defp optional_principal!(%Principal{} = principal, _field), do: principal

  defp optional_principal!(
         %{"tenant_id" => tenant_id, "id" => id, "type" => type},
         field
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(id) and id != "" and
              is_binary(type) and type != "" do
    type = principal_type!(type, field)

    case Principal.new(tenant_id, id, type) do
      {:ok, principal} -> principal
      {:error, :invalid_principal} -> raise ArgumentError, "invalid session #{field}"
    end
  end

  defp optional_principal!(
         %{tenant_id: tenant_id, id: id, type: type},
         field
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(id) and id != "" and
              ((is_binary(type) and type != "") or type in [:user, :agent, :service]) do
    type =
      case type do
        atom when atom in [:user, :agent, :service] -> atom
        string when is_binary(string) -> principal_type!(string, field)
      end

    case Principal.new(tenant_id, id, type) do
      {:ok, principal} -> principal
      {:error, :invalid_principal} -> raise ArgumentError, "invalid session #{field}"
    end
  end

  defp optional_principal!(value, field) do
    raise ArgumentError, "invalid session #{field}: #{inspect(value)}"
  end

  defp principal_type!("user", _field), do: :user
  defp principal_type!("agent", _field), do: :agent
  defp principal_type!("service", _field), do: :service
  defp principal_type!("svc", _field), do: :service

  defp principal_type!(value, field) do
    raise ArgumentError, "invalid session #{field} type: #{inspect(value)}"
  end

  defp resolve_tenant_id!(tenant_id, _creator_principal)
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp resolve_tenant_id!(nil, %Principal{tenant_id: tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp resolve_tenant_id!(nil, nil), do: "service"

  defp resolve_tenant_id!(value, _creator_principal) do
    raise ArgumentError, "invalid session tenant_id: #{inspect(value)}"
  end
end
