defmodule Starcite.Observability.PromEx.MetricsTest do
  use ExUnit.Case, async: false

  alias PromEx.MetricTypes.Polling
  alias Starcite.Observability.PromEx.Metrics
  alias Starcite.Runtime.TestHelper

  @memory_layout_event [:starcite, :memory, :layout]
  @expected_measurement_keys [
    :vm_total_bytes,
    :vm_system_bytes,
    :vm_processes_bytes,
    :vm_processes_used_bytes,
    :vm_processes_overhead_bytes,
    :vm_atom_bytes,
    :vm_atom_used_bytes,
    :vm_atom_unused_bytes,
    :vm_binary_bytes,
    :vm_code_bytes,
    :vm_ets_bytes,
    :vm_persistent_term_bytes,
    :event_queue_bytes,
    :archive_read_cache_bytes,
    :session_store_bytes,
    :event_store_bytes,
    :tracked_local_storage_bytes,
    :event_store_limit_bytes,
    :archive_read_cache_limit_bytes
  ]

  setup do
    TestHelper.reset()
    :ok
  end

  test "execute_memory_layout_metrics emits a complete layout snapshot" do
    handler_id = "memory-layout-#{System.unique_integer([:positive, :monotonic])}"

    :ok =
      :telemetry.attach(
        handler_id,
        @memory_layout_event,
        fn event_name, measurements, metadata, pid ->
          send(pid, {event_name, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok = Metrics.execute_memory_layout_metrics()

    assert_receive {@memory_layout_event, measurements, metadata}, 1_000
    assert metadata.node == Atom.to_string(Node.self())

    Enum.each(@expected_measurement_keys, fn key ->
      assert is_integer(Map.fetch!(measurements, key))
      assert Map.fetch!(measurements, key) >= 0
    end)

    assert measurements.vm_processes_overhead_bytes ==
             measurements.vm_processes_bytes - measurements.vm_processes_used_bytes

    assert measurements.vm_atom_unused_bytes ==
             measurements.vm_atom_bytes - measurements.vm_atom_used_bytes

    assert measurements.event_store_bytes ==
             measurements.event_queue_bytes + measurements.archive_read_cache_bytes

    assert measurements.tracked_local_storage_bytes ==
             measurements.event_store_bytes + measurements.session_store_bytes

    assert measurements.event_store_limit_bytes > 0
    assert measurements.archive_read_cache_limit_bytes > 0
  end

  test "polling_metrics exposes the memory layout gauges" do
    [%Polling{} = polling] = Metrics.polling_metrics(poll_rate: 12_345)

    assert polling.group_name == :starcite_memory_layout_polling_metrics
    assert polling.poll_rate == 12_345

    assert polling.measurements_mfa ==
             {PromEx.MetricTypes.Polling, :safe_polling_runner,
              [{Metrics, :execute_memory_layout_metrics, []}]}

    metric_names = Enum.map(polling.metrics, & &1.name)

    assert [:starcite_memory_layout_vm_total_bytes] in metric_names
    assert [:starcite_memory_layout_vm_ets_bytes] in metric_names
    assert [:starcite_memory_layout_archive_read_cache_bytes] in metric_names
    assert [:starcite_memory_layout_session_store_bytes] in metric_names
    assert [:starcite_memory_layout_tracked_local_storage_bytes] in metric_names
    assert [:starcite_memory_layout_event_store_limit_bytes] in metric_names
    assert length(metric_names) == length(@expected_measurement_keys)
  end
end
