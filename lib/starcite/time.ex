defmodule Starcite.Time do
  @moduledoc """
  Shared UTC timestamp rendering for wire payloads.

  Internal structs often store UTC timestamps as `NaiveDateTime`s, while some
  archive paths already materialize `DateTime`s. This module keeps that
  conversion consistent at JSON/socket boundaries.
  """

  @type utc_datetime :: NaiveDateTime.t() | DateTime.t()

  @spec iso8601_utc!(utc_datetime()) :: String.t()
  def iso8601_utc!(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  def iso8601_utc!(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  def iso8601_utc!(other) do
    raise ArgumentError, "expected UTC DateTime or NaiveDateTime, got: #{inspect(other)}"
  end

  @spec iso8601_utc(term()) :: String.t() | term()
  def iso8601_utc(%NaiveDateTime{} = datetime), do: iso8601_utc!(datetime)
  def iso8601_utc(%DateTime{} = datetime), do: iso8601_utc!(datetime)
  def iso8601_utc(other), do: other
end
