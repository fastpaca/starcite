defmodule Starcite.Repo do
  use Ecto.Repo,
    otp_app: :starcite,
    adapter: Ecto.Adapters.Postgres
end
