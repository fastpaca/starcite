defmodule StarciteWeb.Auth.Policy do
  @moduledoc """
  Action-oriented authorization policy for web endpoints.
  """

  alias Starcite.Auth.Principal

  @type auth :: map()

  @spec can_issue_token(auth()) :: :ok | {:error, :forbidden}
  def can_issue_token(%{kind: kind}) when kind in [:none, :service], do: :ok
  def can_issue_token(_auth), do: {:error, :forbidden}

  @spec can_create_session(auth(), map()) :: {:ok, Principal.t()} | {:error, atom()}
  def can_create_session(%{kind: kind}, %{"creator_principal" => creator_principal})
      when kind in [:none, :service] do
    principal_from_payload(creator_principal)
  end

  def can_create_session(%{kind: kind}, _params) when kind in [:none, :service],
    do: {:error, :invalid_session}

  def can_create_session(%{kind: :principal, principal: %Principal{type: :agent}}, _params),
    do: {:error, :forbidden}

  def can_create_session(
        %{kind: :principal, principal: %Principal{type: :user} = principal, scopes: scopes} =
          auth,
        params
      )
      when is_map(params) and is_list(scopes) do
    with :ok <- reject_creator_override(params),
         :ok <- has_scope(auth, "session:create") do
      {:ok, principal}
    end
  end

  def can_create_session(%{kind: :principal, principal: %Principal{type: :user}}, _params),
    do: {:error, :invalid_session}

  def can_create_session(_auth, _params), do: {:error, :invalid_session}

  @spec can_list_sessions(auth()) ::
          {:ok, :all | %{tenant_id: String.t(), owner_principal_ids: [String.t()]}}
          | {:error, atom()}
  def can_list_sessions(%{kind: kind}) when kind in [:none, :service], do: {:ok, :all}

  def can_list_sessions(%{kind: :principal, principal: %Principal{type: :agent}}),
    do: {:error, :forbidden}

  def can_list_sessions(
        %{kind: :principal, principal: %Principal{type: :user} = principal, scopes: scopes} =
          auth
      )
      when is_list(scopes) do
    with :ok <- has_scope(auth, "session:read"),
         {:ok, scope} <- user_list_scope(principal) do
      {:ok, scope}
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
  def allowed_to_access_session(%{kind: kind}, _session_id) when kind in [:none, :service],
    do: :ok

  def allowed_to_access_session(
        %{kind: :principal, principal: %Principal{} = principal} = auth,
        session_id
      )
      when is_binary(session_id) and session_id != "" do
    case {principal.type, Map.get(auth, :session_ids)} do
      {_type, session_ids} when is_list(session_ids) ->
        if Enum.member?(session_ids, session_id), do: :ok, else: {:error, :forbidden_session}

      {:user, nil} ->
        :ok

      {:agent, nil} ->
        {:error, :forbidden_session}

      _other ->
        {:error, :forbidden_session}
    end
  end

  def allowed_to_access_session(_auth, _session_id), do: {:error, :forbidden_session}

  @spec resolve_event_actor(auth(), String.t() | nil) ::
          {:ok, String.t()} | {:error, :invalid_event}
  def resolve_event_actor(%{kind: :principal, principal: %Principal{} = principal}, nil) do
    {:ok, Principal.actor(principal)}
  end

  def resolve_event_actor(
        %{kind: :principal, principal: %Principal{} = principal},
        requested_actor
      )
      when is_binary(requested_actor) and requested_actor != "" do
    actor = Principal.actor(principal)
    if requested_actor == actor, do: {:ok, actor}, else: {:error, :invalid_event}
  end

  def resolve_event_actor(%{kind: :principal}, _requested_actor), do: {:error, :invalid_event}
  def resolve_event_actor(_auth, actor) when is_binary(actor) and actor != "", do: {:ok, actor}
  def resolve_event_actor(_auth, _actor), do: {:error, :invalid_event}

  @spec attach_principal_metadata(auth(), map()) :: map()
  def attach_principal_metadata(
        %{kind: :principal, principal: %Principal{} = principal},
        metadata
      )
      when is_map(metadata) do
    principal_metadata = %{
      "tenant_id" => principal.tenant_id,
      "principal_type" => Atom.to_string(principal.type),
      "principal_id" => principal.id,
      "actor" => Principal.actor(principal)
    }

    Map.update(metadata, "starcite_principal", principal_metadata, fn
      existing when is_map(existing) -> Map.merge(existing, principal_metadata)
      _other -> principal_metadata
    end)
  end

  def attach_principal_metadata(_auth, metadata) when is_map(metadata), do: metadata

  @spec has_scope(auth(), String.t()) :: :ok | {:error, :forbidden_scope}
  def has_scope(%{kind: kind}, _scope) when kind in [:none, :service], do: :ok

  def has_scope(%{kind: :principal, scopes: scopes}, scope)
      when is_binary(scope) and scope != "" and is_list(scopes) do
    if Enum.member?(scopes, scope), do: :ok, else: {:error, :forbidden_scope}
  end

  def has_scope(_auth, _scope), do: {:error, :forbidden_scope}

  defp session_permission(%{kind: kind}, _session, _scope) when kind in [:none, :service], do: :ok

  defp session_permission(
         %{kind: :principal, principal: %Principal{} = principal} = auth,
         %{id: session_id, creator_principal: %Principal{} = creator_principal},
         scope
       )
       when is_binary(session_id) and session_id != "" and is_binary(scope) and scope != "" do
    with :ok <- has_scope(auth, scope),
         :ok <- same_tenant(principal, creator_principal),
         :ok <-
           principal_session_binding(
             principal,
             Map.get(auth, :session_ids),
             session_id,
             creator_principal
           ) do
      :ok
    end
  end

  defp session_permission(_auth, _session, _scope), do: {:error, :forbidden_session}

  defp same_tenant(%Principal{tenant_id: tenant_id}, %Principal{tenant_id: tenant_id}), do: :ok
  defp same_tenant(_principal, _creator_principal), do: {:error, :forbidden_tenant}

  defp principal_session_binding(
         %Principal{type: :agent},
         session_ids,
         session_id,
         _creator_principal
       )
       when is_list(session_ids) do
    if Enum.member?(session_ids, session_id), do: :ok, else: {:error, :forbidden_session}
  end

  defp principal_session_binding(%Principal{type: :agent}, _session_ids, _session_id, _creator),
    do: {:error, :forbidden_session}

  defp principal_session_binding(
         %Principal{type: :user, id: principal_id},
         _session_ids,
         _session_id,
         %Principal{id: principal_id}
       )
       when is_binary(principal_id) and principal_id != "",
       do: :ok

  defp principal_session_binding(
         %Principal{type: :user},
         session_ids,
         session_id,
         _creator_principal
       )
       when is_list(session_ids) do
    if Enum.member?(session_ids, session_id), do: :ok, else: {:error, :forbidden_session}
  end

  defp principal_session_binding(
         %Principal{type: :user},
         _session_ids,
         _session_id,
         _creator_principal
       ),
       do: {:error, :forbidden_session}

  defp principal_session_binding(_principal, _session_ids, _session_id, _creator_principal),
    do: {:error, :forbidden_session}

  defp reject_creator_override(%{"creator_principal" => _override}),
    do: {:error, :invalid_session}

  defp reject_creator_override(_params), do: :ok

  defp user_list_scope(%Principal{tenant_id: tenant_id, id: principal_id})
       when is_binary(tenant_id) and tenant_id != "" and is_binary(principal_id) and
              principal_id != "" do
    {:ok, %{tenant_id: tenant_id, owner_principal_ids: [principal_id]}}
  end

  defp user_list_scope(_principal), do: {:error, :forbidden}

  defp principal_from_payload(%{
         "tenant_id" => tenant_id,
         "id" => principal_id,
         "type" => principal_type
       })
       when is_binary(tenant_id) and tenant_id != "" and is_binary(principal_id) and
              principal_id != "" and principal_type in ["user", "agent"] do
    type = if(principal_type == "user", do: :user, else: :agent)
    Principal.new(tenant_id, principal_id, type)
  end

  defp principal_from_payload(_creator_principal), do: {:error, :invalid_session}
end
