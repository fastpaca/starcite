defmodule Mix.Tasks.Starcite.TestSetup do
  @shortdoc "Runs test prerequisites for the active archive adapter"

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    reset_routing_store()

    if postgres_archive_mode?() do
      Mix.Task.run("ecto.create", ["--quiet"])
      Mix.Task.run("ecto.migrate", ["--quiet"])
    end
  end

  defp reset_routing_store do
    case Application.get_env(:starcite, :routing_store_dir) do
      value when is_binary(value) and value != "" ->
        File.rm_rf!(value)
        File.mkdir_p!(value)

      _other ->
        :ok
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
