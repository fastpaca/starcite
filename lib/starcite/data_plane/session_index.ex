defmodule Starcite.DataPlane.SessionIndex do
  @moduledoc """
  Data-plane adapter for durable session index writes.
  """

  @spec upsert_session(map()) :: :ok
  def upsert_session(row) when is_map(row) do
    case Starcite.Archive.Store.upsert_session(row) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end
end
