defmodule Starcite.WritePath.Create do
  @moduledoc """
  Session creation orchestration for the write path.

  This module is responsible for resolving the effective creator/tenant,
  dispatching the Raft `:create_session` command, and persisting the archived
  session row used for later hydration.
  """

  alias Starcite.Archive.SessionCatalog
  alias Starcite.Auth.Principal
  alias Starcite.WritePath.CommandRouter
  alias Starcite.Observability.Telemetry
  alias Starcite.WritePath

  @doc """
  Create a session through the write path boundary.
  """
  @spec run(keyword()) :: {:ok, map()} | {:error, term()}
  def run(opts \\ [])

  def run(opts) when is_list(opts) do
    create_session(
      session_id_from_opts(opts),
      Keyword.get(opts, :title),
      Keyword.get(opts, :creator_principal),
      Keyword.get(opts, :tenant_id),
      Keyword.get(opts, :metadata, %{})
    )
  end

  def run(_opts), do: {:error, :invalid_session}

  @doc false
  def run_local(id, title, creator_principal, tenant_id, metadata)
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
             is_map(metadata) do
    CommandRouter.dispatch_local_session(
      id,
      {:create_session, id, title, creator_principal, tenant_id, metadata}
    )
  end

  def run_local(_id, _title, _creator_principal, _tenant_id, _metadata),
    do: {:error, :invalid_session}

  defp create_session(
         id,
         title,
         %Principal{tenant_id: tenant_id} = creator_principal,
         tenant_id,
         metadata
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    persist_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp create_session(
         id,
         title,
         %Principal{tenant_id: tenant_id} = creator_principal,
         nil,
         metadata
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    persist_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp create_session(id, title, nil, tenant_id, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    {:ok, creator_principal} = Principal.new(tenant_id, "service", :service)
    persist_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp create_session(id, title, nil, nil, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) do
    {:ok, creator_principal} = Principal.new("service", "service", :service)
    persist_session(id, title, creator_principal, "service", metadata)
  end

  defp create_session(_id, _title, _creator_principal, _tenant_id, _metadata),
    do: {:error, :invalid_session}

  defp persist_session(id, title, creator_principal, tenant_id, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
              is_map(metadata) do
    command = {:create_session, id, title, creator_principal, tenant_id, metadata}

    result =
      CommandRouter.dispatch_session(
        id,
        &CommandRouter.dispatch_server(&1, command),
        WritePath,
        :create_session_local,
        [id, title, creator_principal, tenant_id, metadata]
      )

    case result do
      {:ok, session} = ok ->
        _ = SessionCatalog.persist_created(session, creator_principal, tenant_id, metadata)
        :ok = Telemetry.session_create(id, tenant_id)
        ok

      other ->
        other
    end
  end

  defp session_id_from_opts(opts) when is_list(opts) do
    case Keyword.get(opts, :id) do
      nil -> "ses_" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      value -> value
    end
  end
end
