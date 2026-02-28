defmodule StarciteWeb.Auth.Policy do
  @moduledoc """
  Claim-driven authorization policy for web endpoints.
  """

  alias Starcite.Auth.Principal
  alias Starcite.Session
  alias StarciteWeb.Auth.Context

  @type auth :: Context.t()

  @spec can_create_session(auth(), map()) :: {:ok, Principal.t() | nil} | {:error, atom()}
  def can_create_session(%Context{kind: :none}, _params), do: {:ok, nil}

  def can_create_session(%Context{tenant_id: tenant_id} = auth, _params)
      when is_binary(tenant_id) and tenant_id != "" do
    with :ok <- has_scope(auth, "session:create") do
      {:ok, auth.principal}
    end
  end

  def can_create_session(_auth, _params), do: {:error, :invalid_session}

  @spec resolve_create_session_id(auth(), String.t() | nil) ::
          {:ok, String.t() | nil} | {:error, atom()}
  def resolve_create_session_id(%Context{kind: :none}, requested_id), do: {:ok, requested_id}

  def resolve_create_session_id(%Context{session_id: nil}, requested_id), do: {:ok, requested_id}

  def resolve_create_session_id(%Context{session_id: session_id}, nil)
      when is_binary(session_id) and session_id != "",
      do: {:ok, session_id}

  def resolve_create_session_id(%Context{session_id: session_id}, requested_id)
      when is_binary(session_id) and session_id != "" and is_binary(requested_id) and
             requested_id != "" do
    if requested_id == session_id, do: {:ok, requested_id}, else: {:error, :forbidden_session}
  end

  def resolve_create_session_id(_auth, _requested_id), do: {:error, :invalid_session}

  @spec can_list_sessions(auth()) :: :ok | {:error, atom()}
  def can_list_sessions(%Context{kind: :none}), do: :ok

  def can_list_sessions(%Context{tenant_id: tenant_id} = auth)
      when is_binary(tenant_id) and tenant_id != "" do
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

  def allowed_to_access_session(%Context{session_id: nil}, session_id)
      when is_binary(session_id) and session_id != "",
      do: :ok

  def allowed_to_access_session(%Context{session_id: session_id}, session_id)
      when is_binary(session_id) and session_id != "",
      do: :ok

  def allowed_to_access_session(%Context{session_id: session_id}, _session_id)
      when is_binary(session_id) and session_id != "",
      do: {:error, :forbidden_session}

  def allowed_to_access_session(_auth, _session_id), do: {:error, :forbidden_session}

  @spec resolve_event_actor(auth(), String.t() | nil) ::
          {:ok, String.t()} | {:error, :invalid_event}
  def resolve_event_actor(%Context{subject: subject}, nil)
      when is_binary(subject) and subject != "" do
    {:ok, subject}
  end

  def resolve_event_actor(%Context{subject: subject}, requested_actor)
      when is_binary(subject) and subject != "" and is_binary(requested_actor) and
             requested_actor != "" do
    if requested_actor == subject, do: {:ok, subject}, else: {:error, :invalid_event}
  end

  def resolve_event_actor(_auth, _requested_actor), do: {:error, :invalid_event}

  @spec attach_principal_metadata(auth(), map()) :: map()
  def attach_principal_metadata(%Context{kind: :none}, metadata) when is_map(metadata),
    do: metadata

  def attach_principal_metadata(
        %Context{kind: :jwt, tenant_id: tenant_id, subject: subject} = auth,
        metadata
      )
      when is_binary(tenant_id) and tenant_id != "" and is_binary(subject) and subject != "" and
             is_map(metadata) do
    principal_metadata =
      %{
        "tenant_id" => tenant_id,
        "subject" => subject,
        "actor" => subject
      }
      |> attach_principal_identity(auth)

    Map.update(metadata, "starcite_principal", principal_metadata, fn
      existing when is_map(existing) -> Map.merge(existing, principal_metadata)
      _other -> principal_metadata
    end)
  end

  def attach_principal_metadata(%Context{kind: :jwt}, _metadata) do
    raise ArgumentError, "invalid jwt auth context for principal metadata"
  end

  @spec has_scope(auth(), String.t()) :: :ok | {:error, :forbidden_scope}
  def has_scope(%Context{scopes: scopes}, scope)
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
         %Context{tenant_id: tenant_id} = auth,
         %Session{id: session_id} = session,
         scope
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(session_id) and
              session_id != "" and is_binary(scope) and scope != "" do
    with :ok <- has_scope(auth, scope),
         :ok <- allowed_to_access_session(auth, session_id),
         {:ok, session_tenant_id} <- session_tenant_id(session),
         :ok <- require_same_tenant(tenant_id, session_tenant_id) do
      :ok
    end
  end

  defp session_permission(_auth, _session, _scope), do: {:error, :forbidden_session}

  defp session_tenant_id(%Session{metadata: %{"tenant_id" => tenant_id}})
       when is_binary(tenant_id) and tenant_id != "" do
    {:ok, tenant_id}
  end

  defp session_tenant_id(%Session{creator_principal: %Principal{tenant_id: tenant_id}})
       when is_binary(tenant_id) and tenant_id != "" do
    {:ok, tenant_id}
  end

  defp session_tenant_id(%Session{}), do: {:error, :forbidden_tenant}

  defp require_same_tenant(tenant_id, tenant_id), do: :ok
  defp require_same_tenant(_expected_tenant, _actual_tenant), do: {:error, :forbidden_tenant}

  defp attach_principal_identity(metadata, %Context{principal: %Principal{} = principal}) do
    Map.merge(metadata, %{
      "principal_type" => Atom.to_string(principal.type),
      "principal_id" => principal.id
    })
  end

  defp attach_principal_identity(metadata, %Context{subject: subject})
       when is_binary(subject) and subject != "" do
    case String.split(subject, ":", parts: 2) do
      [principal_type, principal_id] when principal_type != "" and principal_id != "" ->
        Map.merge(metadata, %{
          "principal_type" => principal_type,
          "principal_id" => principal_id
        })

      _other ->
        raise ArgumentError, "invalid jwt subject format for principal metadata"
    end
  end
end
