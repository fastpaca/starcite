defmodule Mix.Tasks.Bench.SuccessMetricsTest do
  use ExUnit.Case, async: true

  alias Mix.Tasks.Bench.SuccessMetrics

  test "build! extracts ips and latency stats for write/read/rtt" do
    suite = %{
      scenarios: %{
        "write" => %{
          run_time_statistics:
            stats(
              120.0,
              80_000_000,
              40_000_000,
              70_000_000,
              100_000_000,
              120_000_000,
              140_000_000,
              190_000_000
            )
        },
        "read" => %{
          run_time_statistics:
            stats(
              980.0,
              15_000_000,
              8_000_000,
              12_000_000,
              20_000_000,
              30_000_000,
              45_000_000,
              60_000_000
            )
        },
        "rtt" => %{
          run_time_statistics:
            stats(
              75.0,
              90_000_000,
              45_000_000,
              70_000_000,
              120_000_000,
              150_000_000,
              200_000_000,
              260_000_000
            )
        }
      }
    }

    report =
      SuccessMetrics.build!(
        suite,
        [write: "write", read: "read", rtt: "rtt"],
        %{
          read: %{p99_ms: 50.0, p999_ms: 80.0},
          write: %{p99_ms: 150.0, p999_ms: 170.0}
        }
      )

    assert_in_delta report.metrics.write.ips, 120.0, 0.0001
    assert_in_delta report.metrics.read.latency_ms.avg, 15.0, 0.0001
    assert_in_delta report.metrics.read.latency_ms.p95, 20.0, 0.0001
    assert_in_delta report.metrics.read.latency_ms.p99, 30.0, 0.0001
    assert_in_delta report.metrics.read.latency_ms.p999, 45.0, 0.0001
    assert_in_delta report.metrics.rtt.latency_ms.max, 260.0, 0.0001

    assert Enum.all?(report.slo_checks, & &1.passed?)
  end

  test "build! supports benchee suite scenario list shape" do
    suite = %{
      scenarios: [
        %{
          name: "write",
          run_time_data: %{
            statistics:
              stats(
                120.0,
                80_000_000,
                40_000_000,
                70_000_000,
                100_000_000,
                120_000_000,
                140_000_000,
                190_000_000
              )
          }
        },
        %{
          name: "read",
          run_time_data: %{
            statistics:
              stats(
                980.0,
                15_000_000,
                8_000_000,
                12_000_000,
                20_000_000,
                30_000_000,
                45_000_000,
                60_000_000
              )
          }
        },
        %{
          name: "rtt",
          run_time_data: %{
            statistics:
              stats(
                75.0,
                90_000_000,
                45_000_000,
                70_000_000,
                120_000_000,
                150_000_000,
                200_000_000,
                260_000_000
              )
          }
        }
      ]
    }

    report =
      SuccessMetrics.build!(
        suite,
        [write: "write", read: "read", rtt: "rtt"],
        %{
          read: %{p99_ms: 50.0, p999_ms: 80.0},
          write: %{p99_ms: 150.0, p999_ms: 170.0}
        }
      )

    assert_in_delta report.metrics.write.latency_ms.p99, 120.0, 0.0001
    assert_in_delta report.metrics.read.latency_ms.p999, 45.0, 0.0001
    assert_in_delta report.metrics.rtt.latency_ms.max, 260.0, 0.0001
  end

  test "build_from_samples! computes mixed mpmc per-operation stats" do
    report =
      SuccessMetrics.build_from_samples!(
        %{
          write: samples_ns(1..100),
          read: samples_ns(1..40),
          rtt: samples_ns(20..120)
        },
        10.0,
        %{
          read: %{p99_ms: 50.0, p999_ms: 100.0},
          write: %{p99_ms: 150.0, p999_ms: 500.0}
        }
      )

    assert_in_delta report.metrics.write.ips, 10.0, 0.0001
    assert_in_delta report.metrics.write.latency_ms.p99, 99.0, 0.0001
    assert_in_delta report.metrics.write.latency_ms.p999, 100.0, 0.0001
    assert_in_delta report.metrics.read.latency_ms.p99, 40.0, 0.0001
    assert_in_delta report.metrics.rtt.latency_ms.p50, 70.0, 0.0001

    assert Enum.all?(report.slo_checks, & &1.passed?)
  end

  test "raise_on_failures! raises when any SLO check fails" do
    suite = %{
      scenarios: %{
        "write" => %{
          run_time_statistics:
            stats(
              120.0,
              80_000_000,
              40_000_000,
              70_000_000,
              100_000_000,
              170_000_000,
              220_000_000,
              300_000_000
            )
        },
        "read" => %{
          run_time_statistics:
            stats(
              980.0,
              15_000_000,
              8_000_000,
              12_000_000,
              20_000_000,
              55_000_000,
              70_000_000,
              90_000_000
            )
        },
        "rtt" => %{
          run_time_statistics:
            stats(
              75.0,
              90_000_000,
              45_000_000,
              70_000_000,
              120_000_000,
              150_000_000,
              200_000_000,
              260_000_000
            )
        }
      }
    }

    report =
      SuccessMetrics.build!(
        suite,
        [write: "write", read: "read", rtt: "rtt"],
        %{
          read: %{p99_ms: 50.0, p999_ms: 80.0},
          write: %{p99_ms: 150.0, p999_ms: 200.0}
        }
      )

    assert_raise Mix.Error, fn ->
      SuccessMetrics.raise_on_failures!(report)
    end
  end

  test "raise_on_failures! raises for mixed mpmc sample report SLO failures" do
    report =
      SuccessMetrics.build_from_samples!(
        %{
          write: samples_ns(1..100),
          read: samples_ns(1..100),
          rtt: samples_ns(1..100)
        },
        10.0,
        %{
          read: %{p99_ms: 50.0, p999_ms: 80.0},
          write: %{p99_ms: 80.0, p999_ms: 90.0}
        }
      )

    assert_raise Mix.Error, fn ->
      SuccessMetrics.raise_on_failures!(report)
    end
  end

  defp stats(ips, avg, min, p50, p95, p99, p999, max) do
    %{
      ips: ips,
      average: avg,
      minimum: min,
      maximum: max,
      percentiles: %{50 => p50, 95 => p95, 99 => p99, 99.9 => p999}
    }
  end

  defp samples_ns(range) do
    Enum.map(range, fn value_ms -> value_ms * 1_000_000 end)
  end
end
