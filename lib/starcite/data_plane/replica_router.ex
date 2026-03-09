defmodule Starcite.DataPlane.ReplicaRouter do
  @moduledoc """
  Backward-compatible wrapper for `Starcite.ControlPlane.ReplicaRouter`.

  Raft-aware routing is now part of the control plane.
  """

  alias Starcite.ControlPlane.ReplicaRouter

  @spec call_on_replica(
          non_neg_integer(),
          module(),
          atom(),
          [term()],
          module(),
          atom(),
          [term()],
          keyword()
        ) :: term()
  defdelegate call_on_replica(
                group_id,
                remote_module,
                remote_fun,
                remote_args,
                local_module,
                local_fun,
                local_args,
                route_opts \\ []
              ),
              to: ReplicaRouter

  @spec route_target(non_neg_integer(), keyword()) :: {:local, node()} | {:remote, [node()]}
  defdelegate route_target(group_id, opts \\ []), to: ReplicaRouter
end
