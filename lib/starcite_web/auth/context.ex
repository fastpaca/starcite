defmodule StarciteWeb.Auth.Context do
  @moduledoc false

  alias Starcite.Auth.Principal

  @type kind :: :none | :jwt

  @none_principal %Principal{tenant_id: "service", id: "service", type: :service}

  @type t :: %__MODULE__{
          kind: kind(),
          principal: Principal.t(),
          scopes: [String.t()],
          session_id: String.t() | nil,
          expires_at: non_neg_integer() | nil
        }

  defstruct kind: :none,
            principal: @none_principal,
            scopes: [],
            session_id: nil,
            expires_at: nil

  @spec none() :: t()
  def none, do: %__MODULE__{}

  @spec ensure_current(t()) :: :ok | {:error, :token_expired}
  def ensure_current(%__MODULE__{expires_at: expires_at})
      when is_integer(expires_at) and expires_at > 0 do
    if expires_at <= System.system_time(:second), do: {:error, :token_expired}, else: :ok
  end

  def ensure_current(%__MODULE__{}), do: :ok
end
