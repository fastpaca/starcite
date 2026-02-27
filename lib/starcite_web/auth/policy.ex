defmodule StarciteWeb.Auth.Policy do
  @moduledoc """
  Claim-driven authorization policy for web endpoints.
  """

  alias Starcite.Auth.Principal

  @type auth :: map()

  @spec can_create_session(auth(), map()) :: {:ok, Principal.t() | nil} | {:error, atom()}
  def can_create_session(%{tenant_id: tenant_id} = auth, _params)
      when is_binary(tenant_id) and tenant_id != "" do
    with :ok <- has_scope(auth, "session:create") do
      {:ok, Map.get(auth, :principal)}
    end
  end

  def can_create_session(_auth, _params), do: {:error, :invalid_session}

  @spec resolve_create_session_id(auth(), String.t() | nil) ::
          {:ok, String.t() | nil} | {:error, atom()}
  def resolve_create_session_id(%{session_id: nil}, requested_id), do: {:ok, requested_id}

  def resolve_create_session_id(%{session_id: session_id}, nil)
      when is_binary(session_id) and session_id != "",
      do: {:ok, session_id}

  def resolve_create_session_id(%{session_id: session_id}, requested_id)
      when is_binary(session_id) and session_id != "" and is_binary(requested_id) and
             requested_id != "" do
    if requested_id == session_id, do: {:ok, requested_id}, else: {:error, :forbidden_session}
  end

  def resolve_create_session_id(_auth, _requested_id), do: {:error, :invalid_session}

  @spec can_list_sessions(auth()) ::
          {:ok, %{tenant_id: String.t(), session_id: String.t() | nil}} | {:error, atom()}
  def can_list_sessions(%{tenant_id: tenant_id} = auth)
      when is_binary(tenant_id) and tenant_id != "" do
    with :ok <- has_scope(auth, "session:read") do
      {:ok, %{tenant_id: tenant_id, session_id: Map.get(auth, :session_id)}}
    end
  end

  def can_list_sessions(_auth), do: {:error, :forbidden}

  @spec allowed_to_append_session(auth(), map()) :: :ok | {:error, atom()}
  def allowed_to_append_session(auth, session) when is_map(auth) and is_map(session) do
    session_permission(auth, session, "session:append")
  end

  @spec allowed_to_read_session(auth(), map()) :: :ok | {:error, atom()}
  def allowed_to_read_session(auth, session) when is_map(auth) and is_map(session) do
    session_permission(auth, session, "session:read")
  end

  @spec allowed_to_access_session(auth(), String.t()) :: :ok | {:error, :forbidden_session}
  def allowed_to_access_session(%{session_id: nil}, session_id)
      when is_binary(session_id) and session_id != "",
      do: :ok

  def allowed_to_access_session(%{session_id: session_id}, session_id)
      when is_binary(session_id) and session_id != "",
      do: :ok

  def allowed_to_access_session(%{session_id: session_id}, _session_id)
      when is_binary(session_id) and session_id != "",
      do: {:error, :forbidden_session}

  def allowed_to_access_session(_auth, _session_id), do: {:error, :forbidden_session}

  @spec resolve_event_actor(auth(), String.t() | nil) ::
          {:ok, String.t()} | {:error, :invalid_event}
  def resolve_event_actor(%{subject: subject}, nil)
      when is_binary(subject) and subject != "" do
    {:ok, subject}
  end

  def resolve_event_actor(%{subject: subject}, requested_actor)
      when is_binary(subject) and subject != "" and is_binary(requested_actor) and
             requested_actor != "" do
    if requested_actor == subject, do: {:ok, subject}, else: {:error, :invalid_event}
  end

  def resolve_event_actor(_auth, _requested_actor), do: {:error, :invalid_event}

  @spec attach_principal_metadata(auth(), map()) :: map()
  def attach_principal_metadata(
        %{tenant_id: tenant_id, subject: subject} = auth,
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

  def attach_principal_metadata(_auth, metadata) when is_map(metadata), do: metadata

  @spec has_scope(auth(), String.t()) :: :ok | {:error, :forbidden_scope}
  def has_scope(%{scopes: scopes}, scope)
      when is_binary(scope) and scope != "" and is_list(scopes) do
    if Enum.member?(scopes, scope), do: :ok, else: {:error, :forbidden_scope}
  end

  def has_scope(_auth, _scope), do: {:error, :forbidden_scope}

  defp session_permission(
         %{tenant_id: tenant_id} = auth,
         %{id: session_id} = session,
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

  defp session_tenant_id(%{metadata: metadata, creator_principal: creator_principal}) do
    case metadata_tenant_id(metadata) || principal_tenant_id(creator_principal) do
      tenant_id when is_binary(tenant_id) and tenant_id != "" ->
        {:ok, tenant_id}

      _other ->
        {:error, :forbidden_tenant}
    end
  end

  defp session_tenant_id(_session), do: {:error, :forbidden_tenant}

  defp metadata_tenant_id(metadata) when is_map(metadata) do
    case Map.get(metadata, "tenant_id") || Map.get(metadata, :tenant_id) do
      tenant_id when is_binary(tenant_id) and tenant_id != "" -> tenant_id
      _other -> nil
    end
  end

  defp metadata_tenant_id(_metadata), do: nil

  defp principal_tenant_id(%Principal{tenant_id: tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp principal_tenant_id(principal) when is_map(principal) do
    case Map.get(principal, "tenant_id") || Map.get(principal, :tenant_id) do
      tenant_id when is_binary(tenant_id) and tenant_id != "" -> tenant_id
      _other -> nil
    end
  end

  defp principal_tenant_id(_principal), do: nil

  defp require_same_tenant(tenant_id, tenant_id), do: :ok
  defp require_same_tenant(_expected_tenant, _actual_tenant), do: {:error, :forbidden_tenant}

  defp attach_principal_identity(metadata, %{principal: %Principal{} = principal}) do
    Map.merge(metadata, %{
      "principal_type" => Atom.to_string(principal.type),
      "principal_id" => principal.id
    })
  end

  defp attach_principal_identity(metadata, %{subject: subject})
       when is_binary(subject) and subject != "" do
    case String.split(subject, ":", parts: 2) do
      [principal_type, principal_id] when principal_type != "" and principal_id != "" ->
        Map.merge(metadata, %{
          "principal_type" => principal_type,
          "principal_id" => principal_id
        })

      _other ->
        metadata
    end
  end

  defp attach_principal_identity(metadata, _auth), do: metadata
end
