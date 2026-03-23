defmodule Starcite.ReleaseTasks do
  @moduledoc false

  @app :starcite
  alias Starcite.Archive.Adapter.S3
  alias Starcite.Archive.Adapter.S3.{Config, SchemaControl}

  def migrate do
    load_app()

    case archive_adapter() do
      Starcite.Archive.Adapter.Postgres ->
        repos()
        |> Enum.each(fn repo ->
          {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))
        end)

      S3 ->
        ensure_s3_client_apps_started!()
        runtime_opts = Application.get_env(@app, :archive_adapter_opts, [])
        config = Config.build!(runtime_opts, [])

        case SchemaControl.migrate(config, actor: "release") do
          {:ok, _stats} ->
            :ok

          {:error, reason} ->
            raise "S3 schema migration failed during release boot: #{inspect(reason)}"
        end

      _other ->
        :ok
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

  defp ensure_s3_client_apps_started! do
    Enum.each([:req, :ex_aws_s3], fn app ->
      case Application.ensure_all_started(app) do
        {:ok, _started_apps} ->
          :ok

        {:error, reason} ->
          raise "failed to start #{app} for S3 schema migration: #{inspect(reason)}"
      end
    end)
  end
end
