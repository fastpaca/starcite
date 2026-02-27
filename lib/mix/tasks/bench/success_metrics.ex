defmodule Mix.Tasks.Bench.SuccessMetrics do
  @moduledoc false

  @latency_fields [
    {:avg, :average},
    {:min, :minimum},
    {:p50, {:percentile, 50.0}},
    {:p95, {:percentile, 95.0}},
    {:p99, {:percentile, 99.0}},
    {:p999, {:percentile, 99.9}},
    {:max, :maximum}
  ]

  @ordered_operations [:write, :read, :rtt]

  @type operation :: :write | :read | :rtt
  @type percentile :: :p99 | :p999

  @type operation_metrics :: %{
          required(:scenario) => String.t(),
          required(:ips) => float(),
          required(:latency_ms) => %{
            required(:avg) => float(),
            required(:min) => float(),
            required(:p50) => float(),
            required(:p95) => float(),
            required(:p99) => float(),
            required(:p999) => float(),
            required(:max) => float()
          }
        }

  @type slo_target :: %{
          required(:p99_ms) => float(),
          required(:p999_ms) => float()
        }

  @type slo_check :: %{
          required(:operation) => :read | :write,
          required(:percentile) => percentile(),
          required(:actual_ms) => float(),
          required(:target_ms) => float(),
          required(:passed?) => boolean()
        }

  @type t :: %{
          required(:operations) => [operation()],
          required(:metrics) => %{required(operation()) => operation_metrics()},
          required(:slo_checks) => [slo_check()]
        }

  @spec build!(map(), [{operation(), String.t()}], %{
          required(:read) => slo_target(),
          required(:write) => slo_target()
        }) ::
          t()
  def build!(suite, operation_scenarios, slo_targets)
      when is_map(suite) and is_list(operation_scenarios) and is_map(slo_targets) do
    scenarios = fetch_scenarios!(suite)

    metrics =
      operation_scenarios
      |> Enum.map(fn
        {operation, scenario_name}
        when operation in @ordered_operations and is_binary(scenario_name) and scenario_name != "" ->
          {operation, operation_metrics!(scenarios, scenario_name)}

        other ->
          raise ArgumentError,
                "invalid operation_scenarios entry: #{inspect(other)} " <>
                  "(expected {:write|:read|:rtt, non-empty scenario name})"
      end)
      |> Map.new()

    %{
      operations: Enum.map(operation_scenarios, &elem(&1, 0)),
      metrics: metrics,
      slo_checks: evaluate_slos(metrics, slo_targets)
    }
  end

  @spec build_from_samples!(
          %{required(operation()) => [non_neg_integer()]},
          float(),
          %{
            required(:read) => slo_target(),
            required(:write) => slo_target()
          }
        ) :: t()
  def build_from_samples!(operation_latencies_ns, measured_seconds, slo_targets)
      when is_map(operation_latencies_ns) and is_number(measured_seconds) and measured_seconds > 0 and
             is_map(slo_targets) do
    metrics =
      @ordered_operations
      |> Enum.map(fn operation ->
        latencies = fetch_list!(operation_latencies_ns, operation, "operation_latencies_ns")
        {operation, operation_metrics_from_samples!(operation, latencies, measured_seconds)}
      end)
      |> Map.new()

    %{
      operations: @ordered_operations,
      metrics: metrics,
      slo_checks: evaluate_slos(metrics, slo_targets)
    }
  end

  @spec print(t()) :: :ok
  def print(report) when is_map(report) do
    metrics = fetch_map!(report, :metrics, "report")
    operations = Map.get(report, :operations, @ordered_operations)
    slo_checks = fetch_list!(report, :slo_checks, "report")

    IO.puts("Hot-path success metrics (latency in ms):")

    Enum.each(operations, fn operation ->
      operation_metrics = fetch_map!(metrics, operation, "metrics")
      latency_ms = fetch_map!(operation_metrics, :latency_ms, "metrics[#{operation}]")
      ips = fetch_number!(operation_metrics, :ips, "metrics[#{operation}]")

      IO.puts(
        "  #{operation}: ips=#{format_float(ips, 2)} " <>
          "avg=#{format_ms(latency_ms.avg)} " <>
          "min=#{format_ms(latency_ms.min)} " <>
          "p50=#{format_ms(latency_ms.p50)} " <>
          "p95=#{format_ms(latency_ms.p95)} " <>
          "p99=#{format_ms(latency_ms.p99)} " <>
          "p999=#{format_ms(latency_ms.p999)} " <>
          "max=#{format_ms(latency_ms.max)}"
      )
    end)

    IO.puts("Hot-path SLO checks:")

    Enum.each(slo_checks, fn check ->
      operation = fetch_atom!(check, :operation, "slo_check")
      percentile = fetch_atom!(check, :percentile, "slo_check")
      passed? = fetch_boolean!(check, :passed?, "slo_check")
      actual_ms = fetch_number!(check, :actual_ms, "slo_check")
      target_ms = fetch_number!(check, :target_ms, "slo_check")
      status = if passed?, do: "PASS", else: "FAIL"

      IO.puts(
        "  [#{status}] #{operation} #{percentile} < #{format_ms(target_ms)} " <>
          "(actual=#{format_ms(actual_ms)})"
      )
    end)

    :ok
  end

  @spec raise_on_failures!(t()) :: :ok | no_return()
  def raise_on_failures!(report) when is_map(report) do
    failures =
      report
      |> fetch_list!(:slo_checks, "report")
      |> Enum.reject(&Map.get(&1, :passed?, false))

    case failures do
      [] ->
        :ok

      _ ->
        details =
          failures
          |> Enum.map_join(", ", fn check ->
            operation = fetch_atom!(check, :operation, "slo_check")
            percentile = fetch_atom!(check, :percentile, "slo_check")
            actual_ms = fetch_number!(check, :actual_ms, "slo_check")
            target_ms = fetch_number!(check, :target_ms, "slo_check")

            "#{operation} #{percentile}=#{format_ms(actual_ms)} (target < #{format_ms(target_ms)})"
          end)

        Mix.raise("Hot-path benchmark SLO failure(s): #{details}")
    end
  end

  defp operation_metrics!(scenarios, scenario_name)
       when (is_map(scenarios) or is_list(scenarios)) and is_binary(scenario_name) do
    scenario = find_scenario!(scenarios, scenario_name)
    stats = run_time_stats!(scenario, scenario_name)
    ips = fetch_number!(stats, :ips, "run_time_statistics")
    latency_ms = latency_ms!(stats)
    %{scenario: scenario_name, ips: ips, latency_ms: latency_ms}
  end

  defp operation_metrics_from_samples!(operation, latencies_ns, measured_seconds)
       when operation in @ordered_operations and is_list(latencies_ns) and
              is_number(measured_seconds) and measured_seconds > 0 do
    sorted_latencies =
      latencies_ns
      |> Enum.map(fn
        latency when is_integer(latency) and latency >= 0 ->
          latency

        latency when is_float(latency) and latency >= 0.0 ->
          trunc(latency)

        latency ->
          raise ArgumentError, "invalid latency sample for #{operation}: #{inspect(latency)}"
      end)
      |> Enum.sort()

    case sorted_latencies do
      [] ->
        raise ArgumentError, "missing latency samples for #{operation}"

      _ ->
        sample_size = length(sorted_latencies)
        sum_ns = Enum.sum(sorted_latencies) * 1.0
        avg_ns = sum_ns / sample_size
        min_ns = hd(sorted_latencies)
        max_ns = List.last(sorted_latencies)

        latency_ms = %{
          avg: nanos_to_ms(avg_ns),
          min: nanos_to_ms(min_ns),
          p50: percentile_from_sorted!(sorted_latencies, 50.0),
          p95: percentile_from_sorted!(sorted_latencies, 95.0),
          p99: percentile_from_sorted!(sorted_latencies, 99.0),
          p999: percentile_from_sorted!(sorted_latencies, 99.9),
          max: nanos_to_ms(max_ns)
        }

        %{
          scenario: "mpmc_mixed",
          ips: sample_size / measured_seconds,
          latency_ms: latency_ms
        }
    end
  end

  defp find_scenario!(scenarios, scenario_name)
       when is_map(scenarios) and is_binary(scenario_name) do
    fetch_map!(scenarios, scenario_name, "suite.scenarios")
  end

  defp find_scenario!(scenarios, scenario_name)
       when is_list(scenarios) and is_binary(scenario_name) do
    case Enum.find(scenarios, &scenario_name_match?(&1, scenario_name)) do
      nil ->
        raise ArgumentError,
              "missing suite.scenarios entry for #{inspect(scenario_name)} in #{inspect(scenarios)}"

      scenario when is_map(scenario) ->
        scenario
    end
  end

  defp scenario_name_match?(scenario, scenario_name)
       when is_map(scenario) and is_binary(scenario_name) do
    name = Map.get(scenario, :name)
    job_name = Map.get(scenario, :job_name)
    display_name = Map.get(scenario, :display_name)
    name == scenario_name or job_name == scenario_name or display_name == scenario_name
  end

  defp run_time_stats!(scenario, scenario_name)
       when is_map(scenario) and is_binary(scenario_name) do
    case Map.fetch(scenario, :run_time_statistics) do
      {:ok, stats} when is_map(stats) ->
        stats

      {:ok, stats} ->
        raise ArgumentError,
              "invalid scenario #{inspect(scenario_name)}.run_time_statistics: #{inspect(stats)} " <>
                "(expected map)"

      :error ->
        run_time_data = fetch_map!(scenario, :run_time_data, "scenario #{inspect(scenario_name)}")
        statistics = Map.get(run_time_data, :statistics)

        normalize_stats_map!(
          statistics,
          "scenario #{inspect(scenario_name)}.run_time_data.statistics"
        )
    end
  end

  defp normalize_stats_map!(stats, context) when is_map(stats) and is_binary(context), do: stats

  defp normalize_stats_map!(stats, context)
       when is_struct(stats) and is_binary(context) do
    Map.from_struct(stats)
  end

  defp normalize_stats_map!(stats, context) when is_binary(context) do
    raise ArgumentError, "invalid #{context}: #{inspect(stats)} (expected map/struct)"
  end

  defp latency_ms!(stats) when is_map(stats) do
    Enum.reduce(@latency_fields, %{}, fn
      {label, :average}, acc ->
        Map.put(
          acc,
          label,
          stats |> fetch_number!(:average, "run_time_statistics") |> nanos_to_ms()
        )

      {label, :minimum}, acc ->
        Map.put(
          acc,
          label,
          stats |> fetch_number!(:minimum, "run_time_statistics") |> nanos_to_ms()
        )

      {label, :maximum}, acc ->
        Map.put(
          acc,
          label,
          stats |> fetch_number!(:maximum, "run_time_statistics") |> nanos_to_ms()
        )

      {label, {:percentile, percentile}}, acc ->
        percentiles = fetch_map!(stats, :percentiles, "run_time_statistics")
        percentile_value = percentile_value!(percentiles, percentile)
        Map.put(acc, label, nanos_to_ms(percentile_value))
    end)
  end

  defp percentile_value!(percentiles, target) when is_map(percentiles) and is_number(target) do
    maybe_value =
      Enum.find_value(percentiles, fn
        {key, value} when is_number(key) and abs(key - target) < 0.000_001 ->
          value

        {key, value} when is_binary(key) ->
          case Float.parse(key) do
            {parsed, ""} when abs(parsed - target) < 0.000_001 -> value
            _ -> nil
          end

        _ ->
          nil
      end)

    case maybe_value do
      value when is_number(value) ->
        value

      nil ->
        raise ArgumentError,
              "missing percentile #{target} in run_time_statistics.percentiles: " <>
                "#{inspect(percentiles)}"

      other ->
        raise ArgumentError,
              "invalid percentile #{target} value: #{inspect(other)} " <>
                "(expected number)"
    end
  end

  defp percentile_from_sorted!(sorted_latencies, percentile)
       when is_list(sorted_latencies) and is_number(percentile) do
    sample_size = length(sorted_latencies)

    index =
      percentile
      |> Kernel./(100.0)
      |> Kernel.*(sample_size)
      |> Float.ceil()
      |> trunc()
      |> max(1)
      |> min(sample_size)
      |> Kernel.-(1)

    sorted_latencies
    |> Enum.at(index)
    |> nanos_to_ms()
  end

  defp evaluate_slos(metrics, slo_targets) when is_map(metrics) and is_map(slo_targets) do
    [
      slo_check!(metrics, slo_targets, :read, :p99),
      slo_check!(metrics, slo_targets, :read, :p999),
      slo_check!(metrics, slo_targets, :write, :p99),
      slo_check!(metrics, slo_targets, :write, :p999)
    ]
  end

  defp slo_check!(metrics, slo_targets, operation, percentile)
       when operation in [:read, :write] and percentile in [:p99, :p999] do
    target_key = :"#{percentile}_ms"
    operation_metrics = fetch_map!(metrics, operation, "metrics")
    latency_ms = fetch_map!(operation_metrics, :latency_ms, "metrics[#{operation}]")
    operation_target = fetch_map!(slo_targets, operation, "slo_targets")
    actual_ms = fetch_number!(latency_ms, percentile, "metrics[#{operation}].latency_ms")
    target_ms = fetch_number!(operation_target, target_key, "slo_targets[#{operation}]")

    %{
      operation: operation,
      percentile: percentile,
      actual_ms: actual_ms,
      target_ms: target_ms,
      passed?: actual_ms < target_ms
    }
  end

  defp nanos_to_ms(value) when is_number(value), do: value / 1_000_000.0

  defp format_ms(value) when is_number(value), do: "#{format_float(value, 3)}ms"

  defp format_float(value, decimals)
       when is_number(value) and is_integer(decimals) and decimals > 0 do
    value
    |> Kernel.*(1.0)
    |> :erlang.float_to_binary(decimals: decimals)
  end

  defp fetch_scenarios!(suite) when is_map(suite) do
    case Map.fetch(suite, :scenarios) do
      {:ok, scenarios} when is_map(scenarios) or is_list(scenarios) ->
        scenarios

      {:ok, scenarios} ->
        raise ArgumentError,
              "invalid suite.:scenarios: #{inspect(scenarios)} (expected map/list)"

      :error ->
        raise ArgumentError, "missing suite.:scenarios"
    end
  end

  defp fetch_map!(map, key, context) when is_map(map) and is_binary(context) do
    case Map.fetch(map, key) do
      {:ok, value} when is_map(value) ->
        value

      {:ok, value} ->
        raise ArgumentError,
              "invalid #{context}.#{inspect(key)}: #{inspect(value)} (expected map)"

      :error ->
        raise ArgumentError, "missing #{context}.#{inspect(key)}"
    end
  end

  defp fetch_list!(map, key, context) when is_map(map) and is_binary(context) do
    case Map.fetch(map, key) do
      {:ok, value} when is_list(value) ->
        value

      {:ok, value} ->
        raise ArgumentError,
              "invalid #{context}.#{inspect(key)}: #{inspect(value)} (expected list)"

      :error ->
        raise ArgumentError, "missing #{context}.#{inspect(key)}"
    end
  end

  defp fetch_atom!(map, key, context) when is_map(map) and is_binary(context) do
    case Map.fetch(map, key) do
      {:ok, value} when is_atom(value) ->
        value

      {:ok, value} ->
        raise ArgumentError,
              "invalid #{context}.#{inspect(key)}: #{inspect(value)} (expected atom)"

      :error ->
        raise ArgumentError, "missing #{context}.#{inspect(key)}"
    end
  end

  defp fetch_boolean!(map, key, context) when is_map(map) and is_binary(context) do
    case Map.fetch(map, key) do
      {:ok, value} when is_boolean(value) ->
        value

      {:ok, value} ->
        raise ArgumentError,
              "invalid #{context}.#{inspect(key)}: #{inspect(value)} (expected boolean)"

      :error ->
        raise ArgumentError, "missing #{context}.#{inspect(key)}"
    end
  end

  defp fetch_number!(map, key, context) when is_map(map) and is_binary(context) do
    case Map.fetch(map, key) do
      {:ok, value} when is_number(value) ->
        value * 1.0

      {:ok, value} ->
        raise ArgumentError,
              "invalid #{context}.#{inspect(key)}: #{inspect(value)} (expected number)"

      :error ->
        raise ArgumentError, "missing #{context}.#{inspect(key)}"
    end
  end
end
