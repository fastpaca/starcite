defmodule Starcite.Bench.LocalClusterReport do
  @moduledoc false

  @trend_stats %{
    avg: "avg",
    min: "min",
    med: "med",
    max: "max",
    p90: "p(90)",
    p95: "p(95)",
    p99: "p(99)",
    p999: "p(99.9)"
  }

  @spec build(map(), map(), binary() | [binary()]) :: map()
  def build(metadata, k6_summary, docker_stats)
      when is_map(metadata) and is_map(k6_summary) and
             (is_binary(docker_stats) or is_list(docker_stats)) do
    %{
      metadata: metadata,
      k6: summarize_k6(k6_summary),
      docker: summarize_docker_stats(docker_stats)
    }
  end

  @spec read_k6_summary!(Path.t()) :: map()
  def read_k6_summary!(path) when is_binary(path) do
    path
    |> File.read!()
    |> Jason.decode!()
  end

  @spec summarize_k6(map()) :: map()
  def summarize_k6(%{"metrics" => metrics}) when is_map(metrics) do
    %{
      checks_rate: metric_value(metrics, "checks", ["value", "rate"], 0.0),
      dropped_iterations_count: metric_value(metrics, "dropped_iterations", ["count"], 0),
      dropped_iterations_rate: metric_value(metrics, "dropped_iterations", ["rate"], 0.0),
      events_sent_count: metric_value(metrics, "events_sent", ["count"], 0),
      events_sent_rate: metric_value(metrics, "events_sent", ["rate"], 0.0),
      http_req_failed_rate: metric_value(metrics, "http_req_failed", ["value", "rate"], 0.0),
      iterations_count: metric_value(metrics, "iterations", ["count"], 0),
      iterations_rate: metric_value(metrics, "iterations", ["rate"], 0.0),
      append_latency: trend_metric(metrics, "append_latency"),
      append_request_bytes: trend_metric(metrics, "append_request_bytes"),
      event_payload_bytes: trend_metric(metrics, "event_payload_bytes")
    }
  end

  @spec summarize_docker_stats(binary() | [binary()]) :: map()
  def summarize_docker_stats(content) when is_binary(content) do
    content
    |> String.split("\n", trim: true)
    |> summarize_docker_stats()
  end

  def summarize_docker_stats(lines) when is_list(lines) do
    lines
    |> Enum.reduce(%{nodes: [], minio: [], db: []}, fn line, acc ->
      case parse_stats_line(line) do
        {group, mem_mib} ->
          Map.update!(acc, group, &[mem_mib | &1])

        nil ->
          acc
      end
    end)
    |> Map.new(fn {group, values} -> {group, summarize_series(values)} end)
  end

  @spec write_report!(map(), Path.t()) :: :ok
  def write_report!(report, path) when is_map(report) and is_binary(path) do
    path
    |> Path.dirname()
    |> File.mkdir_p!()

    path
    |> File.write!(Jason.encode_to_iodata!(report, pretty: true))

    :ok
  end

  @spec write_summary!(map(), Path.t()) :: :ok
  def write_summary!(report, path) when is_map(report) and is_binary(path) do
    path
    |> Path.dirname()
    |> File.mkdir_p!()

    path
    |> File.write!(format(report))

    :ok
  end

  @spec format(map()) :: binary()
  def format(%{metadata: metadata, k6: k6, docker: docker})
      when is_map(metadata) and is_map(k6) and is_map(docker) do
    [
      "local_cluster_benchmark",
      "project=#{metadata.compose_project}",
      "output_dir=#{metadata.output_dir}",
      "offered_rate=#{metadata.offered_rate}/s duration=#{metadata.duration} sessions=#{metadata.session_count} producers_per_vu=#{metadata.producers_per_vu} pipeline_depth=#{metadata.pipeline_depth}",
      "effective_throughput=#{format_rate(k6.events_sent_rate)} events_sent=#{k6.events_sent_count} dropped_iterations=#{k6.dropped_iterations_count} (#{format_rate(k6.dropped_iterations_rate)}) checks=#{format_percent(k6.checks_rate)} failures=#{format_percent(k6.http_req_failed_rate)}",
      "append_latency avg=#{format_time_ms(k6.append_latency.avg)} med=#{format_time_ms(k6.append_latency.med)} p95=#{format_time_ms(k6.append_latency.p95)} p99=#{format_time_ms(k6.append_latency.p99)} p99.9=#{format_time_ms(k6.append_latency.p999)} max=#{format_time_ms(k6.append_latency.max)}",
      "append_request_bytes avg=#{format_bytes(k6.append_request_bytes.avg)} med=#{format_bytes(k6.append_request_bytes.med)} p95=#{format_bytes(k6.append_request_bytes.p95)} p99=#{format_bytes(k6.append_request_bytes.p99)} p99.9=#{format_bytes(k6.append_request_bytes.p999)} max=#{format_bytes(k6.append_request_bytes.max)}",
      "event_payload_bytes avg=#{format_bytes(k6.event_payload_bytes.avg)} med=#{format_bytes(k6.event_payload_bytes.med)} p95=#{format_bytes(k6.event_payload_bytes.p95)} p99=#{format_bytes(k6.event_payload_bytes.p99)} p99.9=#{format_bytes(k6.event_payload_bytes.p999)} max=#{format_bytes(k6.event_payload_bytes.max)}",
      "node_rss avg=#{format_mib(docker.nodes.avg_mib)} max=#{format_mib(docker.nodes.max_mib)}",
      "minio_rss avg=#{format_mib(docker.minio.avg_mib)} max=#{format_mib(docker.minio.max_mib)}",
      "db_rss avg=#{format_mib(docker.db.avg_mib)} max=#{format_mib(docker.db.max_mib)}",
      "k6_output=#{metadata.k6_output_path}",
      "k6_summary=#{metadata.k6_summary_path}",
      "docker_stats=#{metadata.docker_stats_path}",
      "report_json=#{metadata.report_path}"
    ]
    |> Enum.join("\n")
  end

  defp metric_value(metrics, metric_name, stat_names, default)
       when is_map(metrics) and is_binary(metric_name) and is_list(stat_names) do
    Enum.find_value(stat_names, default, fn stat_name ->
      get_in(metrics, [metric_name, "values", stat_name]) ||
        get_in(metrics, [metric_name, stat_name])
    end)
  end

  defp trend_metric(metrics, metric_name) when is_map(metrics) and is_binary(metric_name) do
    Enum.into(@trend_stats, %{}, fn {name, stat_name} ->
      {name, metric_value(metrics, metric_name, [stat_name], 0.0)}
    end)
  end

  defp parse_stats_line(line) when is_binary(line) do
    case String.split(String.trim(line), "\t", parts: 3) do
      [name, mem_usage, _cpu_percent] ->
        case stats_group(name) do
          nil -> nil
          group -> {group, parse_mib(mem_usage)}
        end

      _ ->
        nil
    end
  end

  defp stats_group(name) when is_binary(name) do
    cond do
      String.contains?(name, "-node") -> :nodes
      String.contains?(name, "-minio-") -> :minio
      String.contains?(name, "-db-") -> :db
      true -> nil
    end
  end

  defp summarize_series([]), do: %{samples: 0, avg_mib: 0.0, max_mib: 0.0}

  defp summarize_series(values) when is_list(values) do
    %{
      samples: length(values),
      avg_mib: Enum.sum(values) / length(values),
      max_mib: Enum.max(values)
    }
  end

  defp parse_mib(mem_usage) when is_binary(mem_usage) do
    mem_usage
    |> String.split("/", parts: 2)
    |> List.first()
    |> String.trim()
    |> then(&Regex.run(~r/^([0-9]+(?:\.[0-9]+)?)([KMG]?i?B)$/, &1))
    |> case do
      [_, value, unit] ->
        {value, ""} = Float.parse(value)

        case unit do
          "GiB" -> value * 1024.0
          "MiB" -> value
          "KiB" -> value / 1024.0
          "B" -> value / 1_048_576.0
          _ -> value
        end

      _ ->
        0.0
    end
  end

  defp format_rate(value) when is_number(value), do: "#{Float.round(as_float(value), 1)}/s"

  defp format_percent(value) when is_number(value),
    do: "#{Float.round(as_float(value) * 100.0, 2)}%"

  defp format_time_ms(value) when is_number(value) and value < 1.0,
    do: "#{Float.round(as_float(value) * 1000.0, 2)}µs"

  defp format_time_ms(value) when is_number(value) and value < 1000.0,
    do: "#{Float.round(as_float(value), 2)}ms"

  defp format_time_ms(value) when is_number(value),
    do: "#{Float.round(as_float(value) / 1000.0, 2)}s"

  defp format_bytes(value) when is_number(value) and value < 1024.0,
    do: "#{Float.round(as_float(value), 1)}B"

  defp format_bytes(value) when is_number(value) and value < 1_048_576.0,
    do: "#{Float.round(as_float(value) / 1024.0, 2)}KiB"

  defp format_bytes(value) when is_number(value),
    do: "#{Float.round(as_float(value) / 1_048_576.0, 2)}MiB"

  defp format_mib(value) when is_number(value), do: "#{Float.round(as_float(value), 1)}MiB"

  defp as_float(value) when is_integer(value), do: value * 1.0
  defp as_float(value) when is_float(value), do: value
end
