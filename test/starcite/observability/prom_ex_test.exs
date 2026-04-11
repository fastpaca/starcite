defmodule Starcite.Observability.PromExTest do
  use ExUnit.Case, async: true

  alias PromEx.Plugins.Beam, as: BeamPlugin
  alias Starcite.Observability.PromEx
  alias Starcite.Observability.PromEx.Metrics

  test "plugins include Beam and Starcite metrics" do
    assert PromEx.plugins() == [
             BeamPlugin,
             Metrics
           ]
  end
end
