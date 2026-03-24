defmodule StarciteWeb.TailSocket do
  @moduledoc """
  Raw WebSocket handler for session tails.

  Emits JSON text frames for replay and live events.
  """

  @behaviour WebSock
  alias StarciteWeb.TailStream

  @impl true
  def init(params), do: TailStream.init(params)

  @impl true
  def handle_in({_payload, opcode: _opcode}, state) do
    # Tail socket is server->client only; inbound frames are ignored.
    {:ok, state}
  end

  @impl true
  def handle_info(message, state), do: adapt_response(TailStream.handle_info(message, state))

  @impl true
  def terminate(_reason, _state), do: :ok

  defp adapt_response({:ok, state}), do: {:ok, state}

  defp adapt_response({:emit, {:events, events}, state}) do
    payload =
      case {state.frame_batch_size, events} do
        {1, [event]} -> Jason.encode!(event)
        _other -> Jason.encode!(events)
      end

    {:push, {:text, payload}, state}
  end

  defp adapt_response({:emit, {:gap, gap}, state}) do
    {:push, {:text, Jason.encode!(gap)}, state}
  end

  defp adapt_response({:close, code, reason, state}) do
    {:stop, :token_expired, {code, reason}, state}
  end

  defp adapt_response({:stop, _reason, state}), do: {:stop, :normal, state}
end
