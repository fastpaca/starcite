defmodule Mix.Tasks.Bench.Corpus do
  use Mix.Task

  alias Starcite.Bench.EventCorpus
  alias Starcite.Storage.EventArchive.S3.Config

  @shortdoc "Build a sanitized benchmark event corpus from the S3 archive"

  @moduledoc """
  Build a sanitized benchmark corpus from archived Starcite S3 event chunks.

  Usage:

      mix bench.corpus
      mix bench.corpus --bucket starcite-archive-123456789012 --output bench/generated/archive-sample.json
      mix bench.corpus --object-limit 12 --event-limit 192

  Options:

    * `--bucket` - S3 bucket to read from. Defaults to `BENCH_EVENT_CORPUS_S3_BUCKET`,
      then `STARCITE_S3_BUCKET`.
    * `--prefix` - archive prefix. Defaults to `BENCH_EVENT_CORPUS_S3_PREFIX`,
      then `STARCITE_S3_PREFIX`, then `starcite`.
    * `--region` - AWS region. Defaults to `BENCH_EVENT_CORPUS_S3_REGION`,
      then `STARCITE_S3_REGION`, then `AWS_REGION`, then `AWS_DEFAULT_REGION`.
    * `--endpoint` - optional custom endpoint for MinIO/R2-style archives.
    * `--profile` - optional AWS shared-config profile for CLI credential export.
    * `--access-key-id` - optional static access key override.
    * `--secret-access-key` - optional static secret override.
    * `--path-style` - optional boolean override for path-style requests.
    * `--output` - output JSON file. Defaults to `bench/generated/archive-sample.json`.
    * `--object-limit` - number of archive objects to sample. Default `8`.
    * `--event-limit` - number of sanitized events to keep in the corpus. Default `128`.
    * `--seed` - optional integer seed for deterministic sampling.

  The written corpus preserves event structure and approximate request sizes
  while masking string values and dropping archive-only fields.
  """

  @switches [
    help: :boolean,
    bucket: :string,
    prefix: :string,
    region: :string,
    endpoint: :string,
    profile: :string,
    access_key_id: :string,
    secret_access_key: :string,
    path_style: :boolean,
    output: :string,
    object_limit: :integer,
    event_limit: :integer,
    seed: :integer
  ]

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {invalid, rest, Keyword.get(opts, :help, false)} do
      {_, _, true} ->
        Mix.shell().info(@moduledoc)

      {[], [], false} ->
        maybe_seed_random(opts[:seed])
        ensure_s3_apps_started!()
        config = archive_config(opts)
        output = Keyword.get(opts, :output, "bench/generated/archive-sample.json")

        case EventCorpus.sample_archive(config,
               object_limit: Keyword.get(opts, :object_limit, 8),
               event_limit: Keyword.get(opts, :event_limit, 128)
             ) do
          {:ok, document} ->
            :ok = EventCorpus.write_document!(document, output)

            Mix.shell().info("""
            wrote benchmark corpus to #{output}
            sampled_object_count=#{document.sampled_object_count}
            sampled_event_count=#{document.sampled_event_count}
            corpus_event_count=#{length(document.events)}
            type_counts=#{inspect(document.type_counts)}
            """)

          {:error, reason} ->
            Mix.raise("failed to build benchmark corpus: #{inspect(reason)}")
        end

      {[], _rest, false} ->
        Mix.raise("unexpected positional arguments: #{Enum.join(rest, ", ")}")

      {_invalid, _rest, false} ->
        Mix.raise("invalid option(s): #{format_invalid(invalid)}")
    end
  end

  defp archive_config(opts) when is_list(opts) do
    profile = Keyword.get(opts, :profile) || env_value("AWS_PROFILE")

    bucket =
      first_present([
        Keyword.get(opts, :bucket),
        env_value("BENCH_EVENT_CORPUS_S3_BUCKET"),
        env_value("STARCITE_S3_BUCKET")
      ])

    region =
      first_present([
        Keyword.get(opts, :region),
        env_value("BENCH_EVENT_CORPUS_S3_REGION"),
        env_value("STARCITE_S3_REGION"),
        detect_bucket_region(bucket, profile),
        env_value("AWS_REGION"),
        env_value("AWS_DEFAULT_REGION")
      ])

    runtime_opts =
      []
      |> put_string_opt(:bucket, bucket)
      |> put_string_opt(
        :prefix,
        Keyword.get(opts, :prefix) || env_value("BENCH_EVENT_CORPUS_S3_PREFIX")
      )
      |> put_string_opt(:prefix, env_value("STARCITE_S3_PREFIX"))
      |> put_string_opt(:region, region)
      |> put_string_opt(
        :endpoint,
        Keyword.get(opts, :endpoint) || env_value("BENCH_EVENT_CORPUS_S3_ENDPOINT")
      )
      |> put_string_opt(:endpoint, env_value("STARCITE_S3_ENDPOINT"))
      |> put_string_opt(:profile, profile)
      |> put_string_opt(
        :access_key_id,
        Keyword.get(opts, :access_key_id) || env_value("BENCH_EVENT_CORPUS_S3_ACCESS_KEY_ID")
      )
      |> put_string_opt(:access_key_id, env_value("STARCITE_S3_ACCESS_KEY_ID"))
      |> put_string_opt(:access_key_id, env_value("AWS_ACCESS_KEY_ID"))
      |> put_string_opt(
        :secret_access_key,
        Keyword.get(opts, :secret_access_key) ||
          env_value("BENCH_EVENT_CORPUS_S3_SECRET_ACCESS_KEY")
      )
      |> put_string_opt(:secret_access_key, env_value("STARCITE_S3_SECRET_ACCESS_KEY"))
      |> put_string_opt(:secret_access_key, env_value("AWS_SECRET_ACCESS_KEY"))
      |> put_string_opt(:security_token, env_value("BENCH_EVENT_CORPUS_S3_SESSION_TOKEN"))
      |> put_string_opt(:security_token, env_value("STARCITE_S3_SESSION_TOKEN"))
      |> put_string_opt(:security_token, env_value("AWS_SESSION_TOKEN"))
      |> put_boolean_opt(:path_style, Keyword.get(opts, :path_style))
      |> maybe_fill_cli_credentials(profile)

    Config.build!(runtime_opts, [])
  end

  defp ensure_s3_apps_started! do
    Enum.each([:req, :ex_aws_s3], fn app ->
      case Application.ensure_all_started(app) do
        {:ok, _started} ->
          :ok

        {:error, reason} ->
          Mix.raise("failed to start #{app} for benchmark corpus build: #{inspect(reason)}")
      end
    end)
  end

  defp maybe_seed_random(nil), do: :ok

  defp maybe_seed_random(seed) when is_integer(seed) do
    :rand.seed(:exsplus, {seed, seed + 1, seed + 2})
    :ok
  end

  defp env_value(name) when is_binary(name) do
    case System.get_env(name) do
      nil ->
        nil

      raw ->
        case String.trim(raw) do
          "" -> nil
          value -> value
        end
    end
  end

  defp put_string_opt(opts, _key, nil) when is_list(opts), do: opts

  defp put_string_opt(opts, key, value)
       when is_list(opts) and is_atom(key) and is_binary(value) do
    Keyword.put_new(opts, key, value)
  end

  defp put_boolean_opt(opts, _key, nil) when is_list(opts), do: opts

  defp put_boolean_opt(opts, key, value)
       when is_list(opts) and is_atom(key) and is_boolean(value) do
    Keyword.put(opts, key, value)
  end

  defp maybe_fill_cli_credentials(opts, profile) when is_list(opts) do
    if is_binary(opts[:access_key_id]) and is_binary(opts[:secret_access_key]) do
      opts
    else
      case export_cli_credentials(profile) do
        {:ok, credentials} ->
          opts
          |> put_string_opt(:access_key_id, credentials[:access_key_id])
          |> put_string_opt(:secret_access_key, credentials[:secret_access_key])
          |> put_string_opt(:security_token, credentials[:security_token])

        :error ->
          opts
      end
    end
  end

  defp export_cli_credentials(profile) do
    args =
      ["configure", "export-credentials", "--format", "process"] ++
        profile_args(profile)

    case System.cmd("aws", args, stderr_to_stdout: true) do
      {output, 0} ->
        with {:ok, decoded} <- Jason.decode(output),
             access_key_id when is_binary(access_key_id) and access_key_id != "" <-
               decoded["AccessKeyId"],
             secret_access_key when is_binary(secret_access_key) and secret_access_key != "" <-
               decoded["SecretAccessKey"] do
          {:ok,
           [
             access_key_id: access_key_id,
             secret_access_key: secret_access_key,
             security_token: blank_to_nil(decoded["SessionToken"])
           ]}
        else
          _ -> :error
        end

      {_output, _status} ->
        :error
    end
  rescue
    _error in ErlangError ->
      :error
  end

  defp profile_args(profile) when is_binary(profile) and profile != "", do: ["--profile", profile]
  defp profile_args(_profile), do: []

  defp blank_to_nil(nil), do: nil

  defp blank_to_nil(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp first_present(values) when is_list(values) do
    Enum.find(values, &(is_binary(&1) and &1 != ""))
  end

  defp detect_bucket_region(nil, _profile), do: nil

  defp detect_bucket_region(bucket, profile) when is_binary(bucket) do
    args = ["s3api", "get-bucket-location", "--bucket", bucket] ++ profile_args(profile)

    case System.cmd("aws", args, stderr_to_stdout: true) do
      {output, 0} ->
        with {:ok, decoded} <- Jason.decode(output) do
          normalize_bucket_region(decoded["LocationConstraint"])
        else
          _ -> nil
        end

      {_output, _status} ->
        nil
    end
  rescue
    _error in ErlangError ->
      nil
  end

  defp normalize_bucket_region(nil), do: "us-east-1"
  defp normalize_bucket_region(""), do: "us-east-1"
  defp normalize_bucket_region("EU"), do: "eu-west-1"

  defp normalize_bucket_region(region) when is_binary(region) do
    case String.trim(region) do
      "" -> nil
      normalized -> normalized
    end
  end

  defp format_invalid(invalid) do
    invalid
    |> Enum.map(fn {name, value} -> "#{name}=#{inspect(value)}" end)
    |> Enum.join(", ")
  end
end
