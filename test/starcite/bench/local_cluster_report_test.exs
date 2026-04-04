defmodule Starcite.Bench.LocalClusterReportTest do
  use ExUnit.Case, async: true

  alias Starcite.Bench.LocalClusterReport

  test "summarize_k6/1 extracts p99 and p99.9 metrics" do
    summary = %{
      "metrics" => %{
        "checks" => %{"values" => %{"rate" => 1.0}},
        "dropped_iterations" => %{"values" => %{"count" => 12, "rate" => 0.77}},
        "events_sent" => %{"values" => %{"count" => 17_979, "rate" => 1161.7}},
        "http_req_failed" => %{"values" => %{"rate" => 0.0}},
        "iterations" => %{"values" => %{"count" => 17_979, "rate" => 1161.7}},
        "append_latency" => %{
          "values" => %{
            "avg" => 0.94,
            "min" => 0.28,
            "med" => 0.67,
            "max" => 17.09,
            "p(90)" => 1.38,
            "p(95)" => 2.0,
            "p(99)" => 4.81,
            "p(99.9)" => 12.77
          }
        },
        "append_request_bytes" => %{
          "values" => %{
            "avg" => 410.6,
            "min" => 251,
            "med" => 269,
            "max" => 1938,
            "p(90)" => 720,
            "p(95)" => 1801,
            "p(99)" => 1934,
            "p(99.9)" => 1938
          }
        },
        "event_payload_bytes" => %{
          "values" => %{
            "avg" => 191.9,
            "min" => 52,
            "med" => 65,
            "max" => 1558,
            "p(90)" => 528,
            "p(95)" => 1422,
            "p(99)" => 1558,
            "p(99.9)" => 1558
          }
        }
      }
    }

    report = LocalClusterReport.summarize_k6(summary)

    assert report.append_latency.p95 == 2.0
    assert report.append_latency.p99 == 4.81
    assert report.append_latency.p999 == 12.77
    assert report.append_request_bytes.p999 == 1938
    assert report.events_sent_rate == 1161.7
    assert report.dropped_iterations_count == 12
  end

  test "summarize_docker_stats/1 aggregates container rss by service group" do
    stats = """
    potato-node1-1\t240.1MiB / 15.65GiB\t25.85%
    potato-node2-1\t236.3MiB / 15.65GiB\t23.13%
    potato-node3-1\t230.5MiB / 15.65GiB\t23.97%
    potato-minio-1\t164.7MiB / 15.65GiB\t0.01%
    potato-db-1\t84.46MiB / 15.65GiB\t0.01%
    """

    summary = LocalClusterReport.summarize_docker_stats(stats)

    assert summary.nodes.samples == 3
    assert_in_delta summary.nodes.avg_mib, 235.63, 0.01
    assert summary.nodes.max_mib == 240.1
    assert summary.minio.max_mib == 164.7
    assert summary.db.avg_mib == 84.46
  end

  test "format/1 includes p99 and p99.9 lines" do
    report = %{
      metadata: %{
        compose_project: "starcite-bench-123",
        duration: "15s",
        docker_stats_path: "/tmp/docker-stats.txt",
        k6_output_path: "/tmp/k6-output.txt",
        k6_summary_path: "/tmp/k6-summary.json",
        offered_rate: 1200,
        output_dir: "/tmp/potato-bench/starcite-bench-123",
        pipeline_depth: 1,
        producers_per_vu: 2,
        report_path: "/tmp/report.json",
        session_count: 64
      },
      k6: %{
        checks_rate: 1.0,
        dropped_iterations_count: 12,
        dropped_iterations_rate: 0.77,
        events_sent_count: 17_979,
        events_sent_rate: 1161.7,
        http_req_failed_rate: 0.0,
        append_latency: %{avg: 0.94, med: 0.67, p95: 2.0, p99: 4.81, p999: 12.77, max: 17.09},
        append_request_bytes: %{avg: 410.6, med: 269, p95: 1801, p99: 1934, p999: 1938, max: 1938},
        event_payload_bytes: %{avg: 191.9, med: 65, p95: 1422, p99: 1558, p999: 1558, max: 1558}
      },
      docker: %{
        nodes: %{avg_mib: 232.8, max_mib: 257.2},
        minio: %{avg_mib: 169.3, max_mib: 198.1},
        db: %{avg_mib: 77.8, max_mib: 85.0}
      }
    }

    summary = LocalClusterReport.format(report)

    assert summary =~ "p99="
    assert summary =~ "p99.9="
    assert summary =~ "effective_throughput=1161.7/s"
    assert summary =~ "node_rss avg=232.8MiB max=257.2MiB"
  end
end
