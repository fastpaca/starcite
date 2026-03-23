defmodule Starcite.Runtime.TestHelper do
  @moduledoc false

  require ExUnit.CaptureLog

  alias Starcite.Routing.Store

  def reset do
    ExUnit.CaptureLog.capture_log(fn ->
      clear_routing_store()
      clear_event_store()
      clear_session_store()
      clear_session_quorum()
      clear_archive_read_cache()
      reset_repo_sandbox_mode()
      Process.sleep(50)
    end)

    :ok
  end

  defp clear_routing_store do
    if Code.ensure_loaded?(Store) and Store.running?() do
      _ = Store.clear()
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

  defp clear_session_quorum do
    if Code.ensure_loaded?(Starcite.DataPlane.SessionQuorum) do
      Starcite.DataPlane.SessionQuorum.clear()
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
