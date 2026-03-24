defmodule Starcite.TimeTest do
  use ExUnit.Case, async: true

  alias Starcite.Time

  test "iso8601_utc!/1 renders naive UTC datetimes" do
    naive = ~N[2026-03-24 12:34:56]

    assert Time.iso8601_utc!(naive) == "2026-03-24T12:34:56Z"
  end

  test "iso8601_utc!/1 renders datetimes" do
    datetime = DateTime.from_naive!(~N[2026-03-24 12:34:56], "Etc/UTC")

    assert Time.iso8601_utc!(datetime) == "2026-03-24T12:34:56Z"
  end

  test "iso8601_utc/1 passes through non-datetimes" do
    assert Time.iso8601_utc("2026-03-24T12:34:56Z") == "2026-03-24T12:34:56Z"
  end

  test "iso8601_utc!/1 fails loudly on invalid input" do
    assert_raise ArgumentError, ~r/expected UTC DateTime or NaiveDateTime/, fn ->
      Time.iso8601_utc!("2026-03-24T12:34:56Z")
    end
  end
end
