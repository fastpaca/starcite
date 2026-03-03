defmodule Mix.Tasks.Starcite.TestSetup do
  @shortdoc "Runs test prerequisites for the active archive adapter"

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    case archive_mode_from_env(System.get_env("STARCITE_ARCHIVE_ADAPTER")) do
      :postgres ->
        Mix.Task.run("ecto.create", ["--quiet"])
        Mix.Task.run("ecto.migrate", ["--quiet"])

      :s3 ->
        Mix.Task.run("starcite.archive.migrate_s3_schema")
    end
  end

  @doc false
  def archive_mode_from_env(nil), do: :postgres

  def archive_mode_from_env(value) when is_binary(value) do
    case String.trim(value) |> String.downcase() do
      "postgres" -> :postgres
      "s3" -> :s3
      other -> raise ArgumentError, "unsupported STARCITE_ARCHIVE_ADAPTER: #{inspect(other)}"
    end
  end
end
