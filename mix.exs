defmodule FleetLM.MixProject do
  use Mix.Project

  def project do
    [
      app: :fleet_lm,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      listeners: [Phoenix.CodeReloader],
      description:
        "FleetLM - Session primitives for AI products: create sessions, append ordered events, and tail from cursor over WebSocket."
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {FleetLM.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:phoenix, "~> 1.8.1"},
      {:phoenix_live_view, "~> 1.0"},
      {:phoenix_ecto, "~> 4.4"},
      {:ecto_sql, "~> 3.10"},
      {:postgrex, "~> 0.17"},
      {:cachex, "~> 3.6"},
      {:prom_ex, "~> 1.11"},
      {:libcluster, "~> 3.3"},
      {:jason, "~> 1.2"},
      {:bandit, "~> 1.5"},
      {:phoenix_pubsub_redis, "~> 3.0"},
      {:redix, "~> 1.5"},
      {:uniq, "~> 0.6"},
      {:finch, "~> 0.19"},
      {:ra, "~> 2.13"},
      {:bypass, "~> 2.1", only: :test}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to install project dependencies and perform other setup tasks, run:
  #
  #     $ mix setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      setup: ["deps.get", "ecto.setup"],
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"],
      precommit: ["compile --warning-as-errors", "deps.unlock --unused", "format", "test"]
    ]
  end
end
