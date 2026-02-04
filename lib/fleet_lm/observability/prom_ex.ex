defmodule FleetLM.Observability.PromEx do
  @moduledoc """
  PromEx metrics for FleetLM runtime and archiver.

  Exposes metrics based on telemetry events emitted by
  FleetLM.Observability.Telemetry and core libraries.

  Note: This is a message substrate - no LLM token budgets or compaction metrics.
  """

  use PromEx, otp_app: :fleet_lm

  @impl true
  def plugins do
    [
      FleetLM.Observability.PromEx.Metrics
    ]
  end

  @impl true
  def dashboards do
    []
  end
end
