defmodule Starcite.Observability.Tenancy do
  @moduledoc false

  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.SessionStore
  alias Starcite.Session

  @unknown_tenant_id "unknown"
  @invalid_tenant_id "invalid"

  @spec label(term()) :: String.t()
  def label(tenant_id) when is_binary(tenant_id) and tenant_id != "", do: tenant_id
  def label(nil), do: @unknown_tenant_id
  def label(""), do: @invalid_tenant_id
  def label(_invalid), do: @invalid_tenant_id

  @spec label_from_session(Session.t()) :: String.t()
  def label_from_session(session), do: session |> from_session() |> label()

  @spec label_from_event(map()) :: String.t()
  def label_from_event(event), do: event |> from_event() |> label()

  @spec label_from_session_id(term()) :: String.t()
  def label_from_session_id(session_id), do: session_id |> from_session_id() |> label()

  @spec from_session(Session.t()) :: String.t() | nil
  def from_session(%Session{metadata: metadata, creator_principal: creator_principal}) do
    metadata_tenant_id(metadata) || principal_tenant_id(creator_principal)
  end

  def from_session(_session), do: nil

  @spec from_event(map()) :: String.t() | nil
  def from_event(%{"tenant_id" => tenant_id}) when is_binary(tenant_id) and tenant_id != "",
    do: tenant_id

  def from_event(%{tenant_id: tenant_id}) when is_binary(tenant_id) and tenant_id != "",
    do: tenant_id

  def from_event(%{"metadata" => metadata}) when is_map(metadata) do
    metadata_tenant_id(metadata) || principal_metadata_tenant_id(metadata)
  end

  def from_event(%{metadata: metadata}) when is_map(metadata) do
    metadata_tenant_id(metadata) || principal_metadata_tenant_id(metadata)
  end

  def from_event(_event), do: nil

  @spec from_session_id(String.t()) :: String.t() | nil
  def from_session_id(session_id) when is_binary(session_id) and session_id != "" do
    case SessionStore.peek_session(session_id) do
      {:ok, session} -> from_session(session)
      :error -> nil
    end
  end

  def from_session_id(_session_id), do: nil

  defp principal_metadata_tenant_id(%{"starcite_principal" => principal_metadata})
       when is_map(principal_metadata),
       do: metadata_tenant_id(principal_metadata)

  defp principal_metadata_tenant_id(%{starcite_principal: principal_metadata})
       when is_map(principal_metadata),
       do: metadata_tenant_id(principal_metadata)

  defp principal_metadata_tenant_id(_metadata), do: nil

  defp metadata_tenant_id(%{"tenant_id" => tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp metadata_tenant_id(%{tenant_id: tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp metadata_tenant_id(_metadata), do: nil

  defp principal_tenant_id(%Principal{tenant_id: tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp principal_tenant_id(_principal), do: nil
end
