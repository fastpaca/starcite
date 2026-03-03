defmodule Mix.Tasks.Starcite.TestSetupTest do
  use ExUnit.Case, async: true

  alias Mix.Tasks.Starcite.TestSetup

  test "archive_mode_from_env defaults to postgres when env is unset" do
    assert :postgres = TestSetup.archive_mode_from_env(nil)
  end

  test "archive_mode_from_env accepts postgres with whitespace and case variations" do
    assert :postgres = TestSetup.archive_mode_from_env("postgres")
    assert :postgres = TestSetup.archive_mode_from_env("  POSTGRES  ")
  end

  test "archive_mode_from_env accepts s3 with whitespace and case variations" do
    assert :s3 = TestSetup.archive_mode_from_env("s3")
    assert :s3 = TestSetup.archive_mode_from_env("  S3  ")
  end

  test "archive_mode_from_env raises on unsupported adapter value" do
    assert_raise ArgumentError, ~r/unsupported STARCITE_ARCHIVE_ADAPTER/, fn ->
      TestSetup.archive_mode_from_env("redis")
    end
  end
end
