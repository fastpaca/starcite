defmodule Starcite.Auth.Principal do
  @moduledoc """
  Core identity model for authenticated principals.

  This module is intentionally identity-only. It does not encode access control
  or authorization decisions.
  """

  @enforce_keys [:tenant_id, :id, :type]
  @derive {Jason.Encoder, only: [:tenant_id, :id, :type]}
  defstruct [:tenant_id, :id, :type]

  @type type :: :user | :agent | :service

  @type t :: %__MODULE__{
          tenant_id: String.t(),
          id: String.t(),
          type: type()
        }

  @spec new(String.t(), String.t(), type()) :: {:ok, t()} | {:error, :invalid_principal}
  def new(tenant_id, id, type)
      when is_binary(tenant_id) and tenant_id != "" and is_binary(id) and id != "" and
             type in [:user, :agent, :service] do
    {:ok, %__MODULE__{tenant_id: tenant_id, id: id, type: type}}
  end

  def new(_tenant_id, _id, _type), do: {:error, :invalid_principal}

  @spec actor(t()) :: String.t()
  def actor(%__MODULE__{type: type, id: id}) when is_atom(type) and is_binary(id) do
    Atom.to_string(type) <> ":" <> id
  end
end
