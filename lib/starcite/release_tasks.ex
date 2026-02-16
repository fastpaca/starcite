defmodule Starcite.ReleaseTasks do
  @moduledoc false

  @app :starcite

  def migrate do
    load_app()

    if archive_adapter() == Starcite.Archive.Adapter.Postgres do
      repos()
      |> Enum.each(fn repo ->
        {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))
      end)
    end

    :ok
  end

  defp load_app do
    Application.load(@app)
  end

  defp repos do
    Application.fetch_env!(@app, :ecto_repos)
  end

  defp archive_adapter do
    Application.get_env(@app, :archive_adapter, Starcite.Archive.Adapter.S3)
  end
end
