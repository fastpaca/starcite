defmodule Mix.Tasks.Starcite.Ops do
  @moduledoc """
  Operator commands for static write-node control-plane state.

  This task affects routing readiness only and never mutates Raft membership.

  Usage:

      mix starcite.ops status
      mix starcite.ops ready-nodes
      mix starcite.ops drain
      mix starcite.ops drain write-1@starcite.internal
      mix starcite.ops undrain
      mix starcite.ops undrain write-1@starcite.internal
      mix starcite.ops group-replicas 42
      mix starcite.ops wait-ready
      mix starcite.ops wait-ready 60000
      mix starcite.ops wait-drained
      mix starcite.ops wait-drained 60000
  """

  use Mix.Task

  alias Starcite.ControlPlane.Ops

  @default_wait_timeout_ms 30_000
  @shortdoc "Control-plane ops for static write-node routing state"

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    case args do
      ["status"] ->
        Ops.status()
        |> inspect(pretty: true, limit: :infinity)
        |> IO.puts()

      ["ready-nodes"] ->
        print_nodes("ready_nodes", Ops.ready_nodes())

      ["drain"] ->
        drain(Node.self())

      ["drain", raw_node] ->
        with {:ok, node} <- parse_node(raw_node) do
          drain(node)
        else
          {:error, reason} -> Mix.raise(reason)
        end

      ["undrain"] ->
        undrain(Node.self())

      ["undrain", raw_node] ->
        with {:ok, node} <- parse_node(raw_node) do
          undrain(node)
        else
          {:error, reason} -> Mix.raise(reason)
        end

      ["group-replicas", raw_group_id] ->
        with {:ok, group_id} <- parse_group_id(raw_group_id) do
          print_nodes("group_#{group_id}_replicas", Ops.group_replicas(group_id))
        else
          {:error, reason} -> Mix.raise(reason)
        end

      ["wait-ready"] ->
        wait_ready(@default_wait_timeout_ms)

      ["wait-ready", raw_timeout_ms] ->
        with {:ok, timeout_ms} <- parse_timeout_ms(raw_timeout_ms) do
          wait_ready(timeout_ms)
        else
          {:error, reason} -> Mix.raise(reason)
        end

      ["wait-drained"] ->
        wait_drained(@default_wait_timeout_ms)

      ["wait-drained", raw_timeout_ms] ->
        with {:ok, timeout_ms} <- parse_timeout_ms(raw_timeout_ms) do
          wait_drained(timeout_ms)
        else
          {:error, reason} -> Mix.raise(reason)
        end

      _ ->
        Mix.raise(usage())
    end
  end

  defp drain(node) do
    :ok = Ops.drain_node(node)
    IO.puts("node #{node} marked draining")
  end

  defp undrain(node) do
    :ok = Ops.undrain_node(node)
    IO.puts("node #{node} marked ready")
  end

  defp print_nodes(label, nodes) when is_binary(label) and is_list(nodes) do
    rendered =
      nodes
      |> Enum.map(&Atom.to_string/1)
      |> Enum.join(",")

    IO.puts("#{label}=#{rendered}")
  end

  defp parse_group_id(raw_group_id) when is_binary(raw_group_id) do
    case Ops.parse_group_id(raw_group_id) do
      {:ok, group_id} ->
        {:ok, group_id}

      {:error, :invalid_group_id} ->
        {:error, "invalid group id #{inspect(raw_group_id)} (expected configured group range)"}
    end
  end

  defp parse_timeout_ms(raw_timeout_ms) when is_binary(raw_timeout_ms) do
    case Integer.parse(String.trim(raw_timeout_ms)) do
      {timeout_ms, ""} when timeout_ms > 0 ->
        {:ok, timeout_ms}

      _ ->
        {:error, "invalid timeout #{inspect(raw_timeout_ms)} (expected positive integer ms)"}
    end
  end

  defp parse_node(raw_node) when is_binary(raw_node) do
    node_name = String.trim(raw_node)
    known_nodes = Ops.known_nodes()

    case Ops.parse_known_node(node_name) do
      {:ok, node} ->
        {:ok, node}

      {:error, :invalid_write_node} ->
        known =
          known_nodes
          |> Enum.map(&Atom.to_string/1)
          |> Enum.join(", ")

        {:error, "unknown node #{inspect(node_name)} (known nodes: #{known})"}
    end
  end

  defp wait_ready(timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0 do
    case Ops.wait_local_ready(timeout_ms) do
      :ok ->
        IO.puts("local_ready=true")

      {:error, :timeout} ->
        Mix.raise("timeout waiting for local_ready after #{timeout_ms}ms")
    end
  end

  defp wait_drained(timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0 do
    case Ops.wait_local_drained(timeout_ms) do
      :ok ->
        IO.puts("local_drained=true")

      {:error, :timeout} ->
        Mix.raise("timeout waiting for local_drained after #{timeout_ms}ms")
    end
  end

  defp usage do
    """
    usage:
      mix starcite.ops status
      mix starcite.ops ready-nodes
      mix starcite.ops drain [node]
      mix starcite.ops undrain [node]
      mix starcite.ops group-replicas <group_id>
      mix starcite.ops wait-ready [timeout_ms]
      mix starcite.ops wait-drained [timeout_ms]
    """
  end
end
