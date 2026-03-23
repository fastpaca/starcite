defmodule Starcite.TestSupport.DistributedRoutingTarget do
  @moduledoc false

  @response_key :distributed_routing_target_response

  def dispatch do
    case Application.get_env(:starcite, @response_key) do
      {:redirect, owner} when is_atom(owner) ->
        {:error, {:not_leader, {:session_owner, owner}}}

      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, reason}

      nil ->
        {:ok, Node.self()}
    end
  end
end
