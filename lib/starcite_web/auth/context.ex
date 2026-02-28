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
end
