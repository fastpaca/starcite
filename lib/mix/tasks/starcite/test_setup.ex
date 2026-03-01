defmodule Mix.Tasks.Starcite.TestSetup do
  @shortdoc "Runs test prerequisites for the active archive adapter"

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    if postgres_archive_mode?() do
      Mix.Task.run("ecto.create", ["--quiet"])
      Mix.Task.run("ecto.migrate", ["--quiet"])
    end
  end

  defp postgres_archive_mode? do
    case System.get_env("STARCITE_ARCHIVE_ADAPTER") do
      nil ->
        true

      value when is_binary(value) ->
        value
        |> String.trim()
        |> String.downcase()
        |> Kernel.==("postgres")
    end
  end
end
