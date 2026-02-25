defmodule Starcite.Runtime.TestHelperTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime.TestHelper

  @raft_data_dir_key :raft_data_dir

  setup do
    original_raft_data_dir = Application.get_env(:starcite, @raft_data_dir_key)

    test_data_dir =
      Path.join("tmp", "runtime_helper_reset_test_#{System.unique_integer([:positive])}")

    Application.put_env(:starcite, @raft_data_dir_key, test_data_dir)
    File.mkdir_p!(Path.join(test_data_dir, "group_0"))
    File.mkdir_p!(Path.join(test_data_dir, "ra_system"))
    File.write!(Path.join(test_data_dir, "group_0/stale.log"), "stale")
    File.write!(Path.join(test_data_dir, "ra_system/stale.meta"), "stale")

    on_exit(fn ->
      restore_env(:starcite, @raft_data_dir_key, original_raft_data_dir)
      TestHelper.reset()
      File.rm_rf(test_data_dir)
    end)

    {:ok, test_data_dir: test_data_dir}
  end

  test "reset removes stale group and ra_system metadata and repins :ra dirs", %{
    test_data_dir: test_data_dir
  } do
    stale_group_file = Path.join(test_data_dir, "group_0/stale.log")
    stale_ra_system_file = Path.join(test_data_dir, "ra_system/stale.meta")
    ra_system_dir = Path.join(test_data_dir, "ra_system")

    assert File.exists?(stale_group_file)
    assert File.exists?(stale_ra_system_file)

    :ok = TestHelper.reset()

    refute File.exists?(Path.join(test_data_dir, "group_0"))
    refute File.exists?(stale_ra_system_file)
    assert File.dir?(ra_system_dir)
    assert Application.get_env(:ra, :data_dir) == String.to_charlist(ra_system_dir)
    assert Application.get_env(:ra, :wal_data_dir) == String.to_charlist(ra_system_dir)
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
