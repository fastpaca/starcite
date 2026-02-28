defmodule StarciteWeb.Auth.Context do
  @moduledoc false

  alias Starcite.Auth.Principal

  @type kind :: :none | :jwt

  @type t :: %__MODULE__{
          kind: kind(),
          claims: map(),
          tenant_id: String.t() | nil,
          scopes: [String.t()],
          session_id: String.t() | nil,
          subject: String.t() | nil,
          principal: Principal.t() | nil,
          expires_at: non_neg_integer() | nil,
          bearer_token: String.t() | nil
        }

  defstruct kind: :none,
            claims: %{},
            tenant_id: nil,
            scopes: [],
            session_id: nil,
            subject: nil,
            principal: nil,
            expires_at: nil,
            bearer_token: nil

  @spec none() :: t()
  def none, do: %__MODULE__{}
end
