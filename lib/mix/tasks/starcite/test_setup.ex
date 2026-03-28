defmodule Mix.Tasks.Starcite.TestSetup do
  @shortdoc "Runs test prerequisites for the session catalog and event archive"

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    reset_routing_store()
    Mix.Task.run("ecto.create", ["--quiet"])
    Mix.Task.run("ecto.migrate", ["--quiet"])
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
end
