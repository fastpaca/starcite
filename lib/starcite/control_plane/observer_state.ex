defmodule Starcite.ControlPlane.ObserverState do
  @moduledoc false

  @type status :: :ready | :suspect | :lost | :draining

  @type node_meta :: %{
          required(:status) => status(),
          required(:changed_at_ms) => integer()
        }

  @type t :: %__MODULE__{
          nodes: %{optional(node()) => node_meta()}
        }

  defstruct nodes: %{}

  @spec new([node()], integer()) :: t()
  def new(visible_nodes, now_ms) when is_list(visible_nodes) and is_integer(now_ms) do
    nodes =
      visible_nodes
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.into(%{}, fn node ->
        {node, %{status: :ready, changed_at_ms: now_ms}}
      end)

    %__MODULE__{nodes: nodes}
  end

  @spec mark_ready(t(), node(), integer()) :: t()
  def mark_ready(%__MODULE__{} = state, node, now_ms) when is_atom(node) and is_integer(now_ms) do
    put_status(state, node, :ready, now_ms)
  end

  @spec mark_suspect(t(), node(), integer()) :: t()
  def mark_suspect(%__MODULE__{} = state, node, now_ms)
      when is_atom(node) and is_integer(now_ms) do
    case Map.get(state.nodes, node) do
      %{status: :draining} -> state
      _ -> put_status(state, node, :suspect, now_ms)
    end
  end

  @spec mark_draining(t(), node(), integer()) :: t()
  def mark_draining(%__MODULE__{} = state, node, now_ms)
      when is_atom(node) and is_integer(now_ms) do
    put_status(state, node, :draining, now_ms)
  end

  @spec refresh(t(), [node()], integer()) :: t()
  def refresh(%__MODULE__{} = state, visible_nodes, now_ms)
      when is_list(visible_nodes) and is_integer(now_ms) do
    visible = MapSet.new(visible_nodes)

    nodes =
      state.nodes
      |> Enum.reduce(%{}, fn {node, meta}, acc ->
        cond do
          MapSet.member?(visible, node) and meta.status in [:suspect, :lost] ->
            Map.put(acc, node, %{status: :ready, changed_at_ms: now_ms})

          MapSet.member?(visible, node) ->
            Map.put(acc, node, meta)

          meta.status == :draining ->
            Map.put(acc, node, meta)

          true ->
            Map.put(acc, node, %{status: :suspect, changed_at_ms: now_ms})
        end
      end)

    nodes =
      visible
      |> Enum.reduce(nodes, fn node, acc ->
        Map.put_new(acc, node, %{status: :ready, changed_at_ms: now_ms})
      end)

    %__MODULE__{state | nodes: nodes}
  end

  @spec advance_timeouts(t(), integer(), pos_integer()) :: t()
  def advance_timeouts(%__MODULE__{} = state, now_ms, suspect_to_lost_ms)
      when is_integer(now_ms) and is_integer(suspect_to_lost_ms) and suspect_to_lost_ms > 0 do
    nodes =
      Enum.into(state.nodes, %{}, fn {node, meta} ->
        case meta do
          %{status: :suspect, changed_at_ms: changed_at_ms}
          when is_integer(changed_at_ms) and now_ms - changed_at_ms >= suspect_to_lost_ms ->
            {node, %{status: :lost, changed_at_ms: now_ms}}

          _ ->
            {node, meta}
        end
      end)

    %__MODULE__{state | nodes: nodes}
  end

  defp put_status(%__MODULE__{} = state, node, status, now_ms) do
    %__MODULE__{
      state
      | nodes: Map.put(state.nodes, node, %{status: status, changed_at_ms: now_ms})
    }
  end
end
