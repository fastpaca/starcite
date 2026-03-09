defmodule Starcite.Runtime.DataPlaneBoundaryTest do
  use ExUnit.Case, async: true

  @hot_path_files [
    "lib/starcite/write_path.ex",
    "lib/starcite/read_path.ex",
    "lib/starcite/archive.ex",
    "lib/starcite/data_plane/session_log.ex",
    "lib/starcite/data_plane/session_quorum.ex",
    "lib/starcite_web/controllers/session_controller.ex",
    "lib/starcite_web/controllers/tail_controller.ex",
    "lib/starcite_web/tail_socket.ex"
  ]

  @forbidden_references [
    ":ra.",
    "LeaseManager",
    "LeaseBootstrap"
  ]

  test "hot path modules are decoupled from raft control-plane internals" do
    Enum.each(@hot_path_files, fn relative_path ->
      source = File.read!(Path.join(File.cwd!(), relative_path))

      Enum.each(@forbidden_references, fn forbidden ->
        refute String.contains?(source, forbidden),
               "#{relative_path} contains forbidden reference #{inspect(forbidden)}"
      end)
    end)
  end
end
