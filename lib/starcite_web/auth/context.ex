defmodule StarciteWeb.Auth.Context do
  @moduledoc """
  Authenticated request identity context at the web boundary.

  This struct is transport-facing and must not leak into core runtime modules.
  """

  alias Starcite.Auth.Principal

  @type token_kind :: :none | :service | :principal

  @type t :: %__MODULE__{
          claims: map(),
          expires_at: pos_integer() | nil,
          bearer_token: String.t() | nil,
          token_kind: token_kind(),
          principal: Principal.t() | nil,
          scopes: [String.t()],
          session_ids: [String.t()] | nil,
          owner_principal_ids: [String.t()] | nil
        }

  defstruct claims: %{},
            expires_at: nil,
            bearer_token: nil,
            token_kind: :none,
            principal: nil,
            scopes: [],
            session_ids: nil,
            owner_principal_ids: nil
end
