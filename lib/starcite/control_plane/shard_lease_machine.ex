defmodule Starcite.ControlPlane.ShardLeaseMachine do
  @moduledoc """
  Minimal Raft machine for control-plane shard lease groups.

  This machine deliberately does not hold session payload state. Its only job is
  to let the control plane run one Raft group per routing shard so the cluster
  can elect a current owner and derive a fencing epoch for routed writes.
  """

  @behaviour :ra_machine

  @type t :: %{group_id: non_neg_integer()}

  @impl true
  def init(%{group_id: group_id}) when is_integer(group_id) and group_id >= 0 do
    %{group_id: group_id}
  end

  def init(_invalid) do
    %{group_id: 0}
  end

  @impl true
  def version, do: 1

  @impl true
  def which_module(0), do: __MODULE__
  def which_module(1), do: __MODULE__

  def which_module(version),
    do: raise(ArgumentError, "unsupported Raft machine version: #{inspect(version)}")

  @impl true
  def state_enter(_ra_state, _state), do: []

  @impl true
  def apply(_meta, _command, state) do
    {state, {:reply, {:error, :unsupported_command}}, []}
  end
end
