defmodule StarciteWeb.Plugs.EdgeStage do
  @moduledoc false

  @behaviour Plug

  import Plug.Conn

  alias Plug.Conn
  alias Starcite.Observability.Telemetry

  @started_at_key :starcite_edge_started_at

  @impl true
  def init(stage) when stage in [:start, :controller_entry], do: stage

  @impl true
  def call(%Conn{} = conn, :start) do
    put_private(conn, @started_at_key, System.monotonic_time())
  end

  def call(%Conn{private: private, method: method} = conn, :controller_entry) do
    case Map.fetch(private, @started_at_key) do
      {:ok, started_at} when is_integer(started_at) ->
        Telemetry.edge_stage(:controller_entry, method, elapsed_ms(started_at))

      _ ->
        :ok
    end

    conn
  end

  defp elapsed_ms(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
  end
end
