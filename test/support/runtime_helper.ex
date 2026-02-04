defmodule FleetLM.Runtime.TestHelper do
  @moduledoc false

  require ExUnit.CaptureLog
  require Logger

  alias FleetLM.Runtime.RaftManager

  def reset do
    ExUnit.CaptureLog.capture_log(fn ->
      # Stop all running Raft groups first
      stop_all_raft_groups()

      # Wait for processes to fully terminate
      Process.sleep(100)

      # Cleanup Raft data directory (test mode only)
      cleanup_raft_test_data()

      # Brief wait to ensure all cleanup completes
      Process.sleep(50)
    end)

    :ok
  end

  defp stop_all_raft_groups do
    # Delete all 256 Raft groups completely
    for group_id <- 0..(RaftManager.num_groups() - 1) do
      server_id = RaftManager.server_id(group_id)
      server_ref = {server_id, Node.self()}

      # First try to delete the cluster (stops and removes from Ra)
      case :ra.delete_cluster([server_ref], 5000) do
        {:ok, _} ->
          :ok

        {:error, _reason} ->
          # If delete fails, try to force stop the server
          case :ra.force_delete_server(:default, server_ref) do
            :ok -> :ok
            {:error, _} -> :ok
          end
      end
    end
  end

  defp cleanup_raft_test_data do
    test_data_dir = Application.get_env(:fleet_lm, :raft_data_dir, "tmp/test_raft")

    if String.contains?(test_data_dir, "test") do
      File.rm_rf(test_data_dir)
    end
  end
end
