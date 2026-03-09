defmodule StarciteWeb.Health do
  @moduledoc false

  alias Starcite.Operations, as: Ops

  @type body :: map()

  @spec live() :: {200, body()}
  def live do
    {200, %{status: "ok"}}
  end

  @spec ready() :: {200 | 503, body()}
  def ready do
    mode = Ops.local_mode() |> Atom.to_string()
    readiness = Ops.local_readiness(refresh?: true)

    if readiness.ready? do
      {200, %{status: "ok", mode: mode}}
    else
      body = %{
        status: "starting",
        mode: mode,
        reason: readiness_reason(readiness.reason, mode)
      }

      {503, put_detail(body, readiness.detail)}
    end
  end

  defp readiness_reason(:draining, _mode), do: "draining"
  defp readiness_reason(:observer_sync, "routing_node"), do: "observer_sync"
  defp readiness_reason(_reason, "routing_node"), do: "lease_sync"
  defp readiness_reason(_reason, _mode), do: "routing_sync"

  defp put_detail(body, detail) when map_size(detail) == 0, do: body
  defp put_detail(body, detail), do: Map.put(body, :detail, detail)
end
