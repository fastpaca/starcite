defmodule StarciteWeb.Auth.Policy do
  @moduledoc """
  Claim-driven authorization policy for web endpoints.
  """

  alias Starcite.Auth.Principal
  alias Starcite.Session
  alias StarciteWeb.Auth.Context

  @type auth :: Context.t()

  @spec can_create_session(auth(), map()) :: {:ok, Principal.t()} | {:error, atom()}
  def can_create_session(%Context{kind: :none, principal: %Principal{} = principal}, _params),
    do: {:ok, principal}

  def can_create_session(
        %Context{kind: :jwt, principal: %Principal{} = principal} = auth,
        _params
      ) do
    with :ok <- has_scope(auth, "session:create") do
      {:ok, principal}
    end
  end

  def can_create_session(_auth, _params), do: {:error, :invalid_session}

  @spec resolve_create_session_id(auth(), String.t() | nil) ::
          {:ok, String.t() | nil} | {:error, atom()}
  def resolve_create_session_id(%Context{kind: :none}, requested_id), do: {:ok, requested_id}

  def resolve_create_session_id(%Context{kind: :jwt, session_id: nil}, requested_id),
    do: {:ok, requested_id}

  def resolve_create_session_id(%Context{kind: :jwt, session_id: session_id}, nil)
      when is_binary(session_id) and session_id != "",
      do: {:ok, session_id}

  def resolve_create_session_id(%Context{kind: :jwt, session_id: session_id}, requested_id)
      when is_binary(session_id) and session_id != "" and is_binary(requested_id) and
             requested_id != "" do
    if requested_id == session_id, do: {:ok, requested_id}, else: {:error, :forbidden_session}
  end

  def resolve_create_session_id(_auth, _requested_id), do: {:error, :invalid_session}

  @spec can_list_sessions(auth()) :: :ok | {:error, atom()}
  def can_list_sessions(%Context{kind: :none}), do: :ok

  def can_list_sessions(%Context{kind: :jwt, principal: %Principal{}} = auth) do
    has_scope(auth, "session:read")
  end

  def can_list_sessions(_auth), do: {:error, :forbidden}

  @spec allowed_to_append_session(auth(), Session.t()) :: :ok | {:error, atom()}
  def allowed_to_append_session(%Context{} = auth, %Session{} = session) do
    session_permission(auth, session, "session:append")
  end

  @spec allowed_to_read_session(auth(), Session.t()) :: :ok | {:error, atom()}
  def allowed_to_read_session(%Context{} = auth, %Session{} = session) do
    session_permission(auth, session, "session:read")
  end

  @spec allowed_to_access_session(auth(), String.t()) :: :ok | {:error, :forbidden_session}
  def allowed_to_access_session(%Context{kind: :none}, session_id)
      when is_binary(session_id) and session_id != "",
      do: :ok

  def allowed_to_access_session(%Context{kind: :jwt, session_id: nil}, session_id)
      when is_binary(session_id) and session_id != "",
      do: :ok

  def allowed_to_access_session(%Context{kind: :jwt, session_id: session_id}, session_id)
      when is_binary(session_id) and session_id != "",
      do: :ok

  def allowed_to_access_session(%Context{kind: :jwt, session_id: session_id}, _session_id)
      when is_binary(session_id) and session_id != "",
      do: {:error, :forbidden_session}

  def allowed_to_access_session(_auth, _session_id), do: {:error, :forbidden_session}

  @spec resolve_event_actor(auth(), String.t() | nil) ::
          {:ok, String.t()} | {:error, :invalid_event}
  def resolve_event_actor(%Context{principal: %Principal{} = principal}, nil) do
    {:ok, Principal.actor(principal)}
  end

  def resolve_event_actor(%Context{principal: %Principal{} = principal}, requested_actor)
      when is_binary(requested_actor) and requested_actor != "" do
    actor = Principal.actor(principal)
    if requested_actor == actor, do: {:ok, actor}, else: {:error, :invalid_event}
  end

  def resolve_event_actor(_auth, _requested_actor), do: {:error, :invalid_event}

  @spec attach_principal_metadata(auth(), map()) :: map()
  def attach_principal_metadata(%Context{principal: %Principal{} = principal}, metadata)
      when is_map(metadata) do
    principal_metadata = %{
      "tenant_id" => principal.tenant_id,
      "actor" => Principal.actor(principal),
      "principal_type" => Atom.to_string(principal.type),
      "principal_id" => principal.id
    }

    Map.update(metadata, "starcite_principal", principal_metadata, fn
      existing when is_map(existing) -> Map.merge(existing, principal_metadata)
      _other -> principal_metadata
    end)
  end

  @spec has_scope(auth(), String.t()) :: :ok | {:error, :forbidden_scope}
  def has_scope(%Context{kind: :jwt, scopes: scopes}, scope)
      when is_binary(scope) and scope != "" and is_list(scopes) do
    if Enum.member?(scopes, scope), do: :ok, else: {:error, :forbidden_scope}
  end

  def has_scope(_auth, _scope), do: {:error, :forbidden_scope}

  defp session_permission(
         %Context{kind: :none},
         %Session{id: session_id},
         _scope
       )
       when is_binary(session_id) and session_id != "",
       do: :ok

  defp session_permission(
         %Context{kind: :jwt, principal: %Principal{tenant_id: tenant_id}} = auth,
         %Session{id: session_id, tenant_id: session_tenant_id},
         scope
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(session_id) and
              session_id != "" and is_binary(scope) and scope != "" and
              is_binary(session_tenant_id) and session_tenant_id != "" do
    with :ok <- has_scope(auth, scope),
         :ok <- allowed_to_access_session(auth, session_id),
         :ok <- require_same_tenant(tenant_id, session_tenant_id) do
      :ok
    end
  end

  defp session_permission(_auth, _session, _scope), do: {:error, :forbidden_session}

  defp require_same_tenant(tenant_id, tenant_id), do: :ok
  defp require_same_tenant(_expected_tenant, _actual_tenant), do: {:error, :forbidden_tenant}
end
