defmodule Starcite.Runtime.TestHelper do
  @moduledoc false

  require ExUnit.CaptureLog
  require Logger

  alias Starcite.DataPlane.RaftManager

  def reset do
    ExUnit.CaptureLog.capture_log(fn ->
      # Stop all running Raft groups first
      stop_all_raft_groups()

      # Wait for processes to fully terminate
      Process.sleep(100)

      # Restart :ra around cleanup so we can safely clear ra_system metadata.
      stop_ra_application()
      cleanup_raft_test_data()
      cleanup_ra_default_directory()
      configure_ra_system_storage()
      :ok = :ra.start()
      :logger.set_application_level(:ra, :error)

      # Cleanup ETS event mirror store (when present)
      clear_event_store()
      clear_session_store()
      clear_session_owners()
      clear_archive_read_cache()
      reset_repo_sandbox_mode()

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
    test_data_dir = Application.get_env(:starcite, :raft_data_dir, "tmp/test_raft")

    if String.contains?(test_data_dir, "test") do
      test_data_dir
      |> Path.join("group_*")
      |> Path.wildcard()
      |> Enum.each(&File.rm_rf/1)

      test_data_dir
      |> Path.join("ra_system")
      |> File.rm_rf()
    end
  end

  defp cleanup_ra_default_directory do
    ra_default_dir = Path.join(File.cwd!(), "nonode@nohost")

    if File.dir?(ra_default_dir) and String.ends_with?(ra_default_dir, "nonode@nohost") do
      File.rm_rf(ra_default_dir)
    end
  end

  defp stop_ra_application do
    case Application.stop(:ra) do
      :ok ->
        :ok

      {:error, {:not_started, :ra}} ->
        :ok

      {:error, reason} ->
        raise ArgumentError, "failed to stop :ra during test reset: #{inspect(reason)}"
    end
  end

  defp configure_ra_system_storage do
    ra_system_dir = RaftManager.ra_system_data_dir()
    wal_data_dir = RaftManager.ra_wal_data_dir()

    with :ok <- File.mkdir_p(ra_system_dir),
         :ok <- File.mkdir_p(wal_data_dir) do
      Application.put_env(:ra, :data_dir, String.to_charlist(ra_system_dir))
      Application.put_env(:ra, :wal_data_dir, String.to_charlist(wal_data_dir))
      :ok
    else
      {:error, reason} ->
        raise ArgumentError,
              "failed to prepare :ra storage directories #{inspect(ra_system_dir)} / #{inspect(wal_data_dir)} during test reset: #{inspect(reason)}"
    end
  end

  defp clear_event_store do
    if Code.ensure_loaded?(Starcite.DataPlane.EventStore) do
      Starcite.DataPlane.EventStore.clear()
    end
  end

  defp clear_session_store do
    if Code.ensure_loaded?(Starcite.DataPlane.SessionStore) do
      Starcite.DataPlane.SessionStore.clear()
    end
  end

  defp clear_session_owners do
    if Code.ensure_loaded?(Starcite.DataPlane.SessionOwners) do
      Starcite.DataPlane.SessionOwners.clear()
    end
  end

  defp clear_archive_read_cache do
    if Process.whereis(:starcite_archive_read_cache) do
      _ = Cachex.clear(:starcite_archive_read_cache)
    end
  end

  defp reset_repo_sandbox_mode do
    if Code.ensure_loaded?(Starcite.Repo) do
      Ecto.Adapters.SQL.Sandbox.mode(Starcite.Repo, :auto)
    end
  rescue
    _ -> :ok
  end
end
