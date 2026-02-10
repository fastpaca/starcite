defmodule Starcite.Observability.PromEx do
  @moduledoc """
  PromEx metrics for Starcite runtime and archiver.

  Exposes metrics based on telemetry events emitted by
  Starcite.Observability.Telemetry and core libraries.

  Note: This is an event substrate - no LLM token budgets or compaction metrics.
  """

  use PromEx, otp_app: :starcite

  @impl true
  def plugins do
    [
      Starcite.Observability.PromEx.Metrics,
      PromEx.Plugins.Beam
    ]
  end

  @impl true
  def dashboards do
    []
  end
end
