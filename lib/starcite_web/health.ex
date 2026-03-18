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

      if map_size(readiness.detail) == 0 do
        {503, body}
      else
        {503, Map.put(body, :detail, readiness.detail)}
      end
    end
  end

  defp readiness_reason(:draining, _mode), do: "draining"
  defp readiness_reason(:lease_expired, _mode), do: "lease_expired"
  defp readiness_reason(_reason, _mode), do: "routing_sync"
end
