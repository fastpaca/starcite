defmodule FleetLM.Repo do
  use Ecto.Repo,
    otp_app: :fleet_lm,
    adapter: Ecto.Adapters.Postgres
end
