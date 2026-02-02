defmodule Fastpaca.Observability.PromEx do
  @moduledoc """
  PromEx metrics for Fastpaca runtime and archiver.

  Exposes metrics based on telemetry events emitted by
  Fastpaca.Observability.Telemetry and core libraries.

  Note: This is a message substrate - no LLM token budgets or compaction metrics.
  """

  use PromEx, otp_app: :fastpaca

  @impl true
  def plugins do
    [
      Fastpaca.Observability.PromEx.Metrics
    ]
  end

  @impl true
  def dashboards do
    []
  end
end
