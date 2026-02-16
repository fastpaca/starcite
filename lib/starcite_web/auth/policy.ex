defmodule StarciteWeb.Auth.Policy do
  @moduledoc """
  Action-oriented authorization policy for web endpoints.
  """

  alias Starcite.Auth.Principal

  @type auth :: map()

  @spec authorize_issue_token(auth()) :: :ok | {:error, :forbidden}
  def authorize_issue_token(%{kind: kind}) when kind in [:none, :service], do: :ok
  def authorize_issue_token(_auth), do: {:error, :forbidden}

  @spec authorize_create_session(auth(), map()) :: {:ok, Principal.t()} | {:error, atom()}
  def authorize_create_session(%{kind: kind}, %{"creator_principal" => creator_principal})
      when kind in [:none, :service] do
    principal_from_payload(creator_principal)
  end

  def authorize_create_session(%{kind: kind}, _params) when kind in [:none, :service],
    do: {:error, :invalid_session}

  def authorize_create_session(
        %{kind: :principal, principal: %Principal{type: :user} = principal} = auth,
        params
      )
      when is_map(params) do
    with :ok <- authorize_scope(auth, "session:create"),
         :ok <- reject_creator_override(params["creator_principal"]) do
      {:ok, principal}
    end
  end

  def authorize_create_session(%{kind: :principal, principal: %Principal{type: :agent}}, _params),
    do: {:error, :forbidden}

  def authorize_create_session(_auth, _params), do: {:error, :invalid_session}

  @spec authorize_list_sessions(auth()) ::
          {:ok, :all | %{tenant_id: String.t(), owner_principal_ids: [String.t()]}}
          | {:error, atom()}
  def authorize_list_sessions(%{kind: kind}) when kind in [:none, :service], do: {:ok, :all}

  def authorize_list_sessions(%{kind: :principal, principal: %Principal{type: :agent}}),
    do: {:error, :forbidden}

  def authorize_list_sessions(
        %{
          kind: :principal,
          principal: %Principal{type: :user, tenant_id: tenant_id, id: principal_id}
        } = auth
      )
      when is_binary(tenant_id) and tenant_id != "" and is_binary(principal_id) and
             principal_id != "" do
    owner_principal_ids = Map.get(auth, :owner_principal_ids, [])

    with :ok <- authorize_scope(auth, "session:read"),
         {:ok, owner_principal_ids} <- normalize_owner_principal_ids(owner_principal_ids) do
      {:ok,
       %{
         tenant_id: tenant_id,
         owner_principal_ids: Enum.uniq([principal_id | owner_principal_ids])
       }}
    end
  end

  def authorize_list_sessions(_auth), do: {:error, :forbidden}

  @spec authorize_append(auth(), map()) :: :ok | {:error, atom()}
  def authorize_append(auth, session) when is_map(auth) and is_map(session) do
    authorize_session_action(auth, session, "session:append")
  end

  @spec authorize_tail(auth(), map()) :: :ok | {:error, atom()}
  def authorize_tail(auth, session) when is_map(auth) and is_map(session) do
    authorize_session_action(auth, session, "session:read")
  end

  @spec authorize_session_reference(auth(), String.t()) :: :ok | {:error, :forbidden_session}
  def authorize_session_reference(%{kind: kind}, _session_id) when kind in [:none, :service],
    do: :ok

  def authorize_session_reference(
        %{kind: :principal, principal: %Principal{type: :agent}, session_ids: session_ids},
        session_id
      )
      when is_binary(session_id) and session_id != "" and is_list(session_ids) do
    if Enum.member?(session_ids, session_id), do: :ok, else: {:error, :forbidden_session}
  end

  def authorize_session_reference(
        %{kind: :principal, principal: %Principal{type: :agent}},
        _session_id
      ),
      do: {:error, :forbidden_session}

  def authorize_session_reference(
        %{kind: :principal, principal: %Principal{type: :user}, session_ids: session_ids},
        session_id
      )
      when is_binary(session_id) and session_id != "" and is_list(session_ids) do
    if Enum.member?(session_ids, session_id), do: :ok, else: {:error, :forbidden_session}
  end

  def authorize_session_reference(
        %{kind: :principal, principal: %Principal{type: :user}},
        session_id
      )
      when is_binary(session_id) and session_id != "",
      do: :ok

  def authorize_session_reference(%{kind: :principal}, _session_id),
    do: {:error, :forbidden_session}

  def authorize_session_reference(_auth, _session_id), do: {:error, :forbidden_session}

  @spec resolve_actor(auth(), String.t() | nil) :: {:ok, String.t()} | {:error, :invalid_event}
  def resolve_actor(%{kind: :principal, principal: %Principal{} = principal}, nil) do
    {:ok, Principal.actor(principal)}
  end

  def resolve_actor(
        %{kind: :principal, principal: %Principal{} = principal},
        requested_actor
      )
      when is_binary(requested_actor) and requested_actor != "" do
    actor = Principal.actor(principal)
    if requested_actor == actor, do: {:ok, actor}, else: {:error, :invalid_event}
  end

  def resolve_actor(%{kind: :principal}, _requested_actor), do: {:error, :invalid_event}
  def resolve_actor(_auth, actor) when is_binary(actor) and actor != "", do: {:ok, actor}
  def resolve_actor(_auth, _actor), do: {:error, :invalid_event}

  @spec stamp_event_metadata(auth(), map()) :: map()
  def stamp_event_metadata(%{kind: :principal, principal: %Principal{} = principal}, metadata)
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

  def stamp_event_metadata(_auth, metadata) when is_map(metadata), do: metadata

  @spec authorize_scope(auth(), String.t()) :: :ok | {:error, :forbidden_scope}
  def authorize_scope(%{kind: kind}, _scope) when kind in [:none, :service], do: :ok

  def authorize_scope(%{kind: :principal, scopes: scopes}, scope)
      when is_binary(scope) and scope != "" and is_list(scopes) do
    if Enum.member?(scopes, scope), do: :ok, else: {:error, :forbidden_scope}
  end

  def authorize_scope(_auth, _scope), do: {:error, :forbidden_scope}

  defp authorize_session_action(%{kind: kind}, _session, _scope) when kind in [:none, :service],
    do: :ok

  defp authorize_session_action(
         %{kind: :principal, principal: %Principal{} = principal} = auth,
         session,
         scope
       )
       when is_map(session) and is_binary(scope) and scope != "" do
    with :ok <- authorize_scope(auth, scope),
         {:ok, creator_principal} <- session_creator_principal(session),
         :ok <- authorize_tenant(principal, creator_principal),
         :ok <- authorize_session_binding(auth, session_id(session), creator_principal) do
      :ok
    end
  end

  defp authorize_session_action(_auth, _session, _scope), do: {:error, :forbidden_session}

  defp authorize_tenant(%Principal{tenant_id: tenant_id}, %Principal{tenant_id: tenant_id}),
    do: :ok

  defp authorize_tenant(_principal, _creator_principal), do: {:error, :forbidden_tenant}

  defp authorize_session_binding(
         %{principal: %Principal{type: :agent}, session_ids: session_ids},
         session_id,
         _creator_principal
       )
       when is_binary(session_id) and session_id != "" and is_list(session_ids) do
    if Enum.member?(session_ids, session_id), do: :ok, else: {:error, :forbidden_session}
  end

  defp authorize_session_binding(
         %{principal: %Principal{type: :agent}},
         _session_id,
         _creator_principal
       ),
       do: {:error, :forbidden_session}

  defp authorize_session_binding(
         %{principal: %Principal{type: :user, id: principal_id}, session_ids: session_ids},
         session_id,
         _creator_principal
       )
       when is_binary(principal_id) and principal_id != "" and is_binary(session_id) and
              session_id != "" and is_list(session_ids) do
    if Enum.member?(session_ids, session_id), do: :ok, else: {:error, :forbidden_session}
  end

  defp authorize_session_binding(
         %{principal: %Principal{type: :user, id: principal_id}} = auth,
         _session_id,
         %Principal{id: creator_principal_id}
       )
       when is_binary(principal_id) and principal_id != "" and is_binary(creator_principal_id) and
              creator_principal_id != "" do
    owner_principal_ids = Map.get(auth, :owner_principal_ids, [])

    with {:ok, owner_principal_ids} <- normalize_owner_principal_ids(owner_principal_ids) do
      if creator_principal_id == principal_id or
           Enum.member?(owner_principal_ids, creator_principal_id) do
        :ok
      else
        {:error, :forbidden_session}
      end
    end
  end

  defp authorize_session_binding(_auth, _session_id, _creator_principal),
    do: {:error, :forbidden_session}

  defp session_creator_principal(%{creator_principal: %Principal{} = principal}),
    do: {:ok, principal}

  defp session_creator_principal(%{creator_principal: creator_principal})
       when is_map(creator_principal) do
    principal_from_payload(creator_principal)
  end

  defp session_creator_principal(_session), do: {:error, :forbidden_session}

  defp session_id(%{id: id}) when is_binary(id) and id != "", do: id
  defp session_id(%{"id" => id}) when is_binary(id) and id != "", do: id
  defp session_id(_session), do: nil

  defp principal_from_payload(%{
         "tenant_id" => tenant_id,
         "id" => principal_id,
         "type" => principal_type
       }) do
    with {:ok, tenant_id} <- required_non_empty_string(tenant_id),
         {:ok, principal_id} <- required_non_empty_string(principal_id),
         {:ok, principal_type} <- principal_type(principal_type),
         {:ok, principal} <- Principal.new(tenant_id, principal_id, principal_type) do
      {:ok, principal}
    else
      {:error, _reason} -> {:error, :invalid_session}
    end
  end

  defp principal_from_payload(_creator_principal), do: {:error, :invalid_session}

  defp reject_creator_override(nil), do: :ok
  defp reject_creator_override(_override), do: {:error, :invalid_session}

  defp normalize_owner_principal_ids(nil), do: {:ok, []}

  defp normalize_owner_principal_ids(owner_principal_ids) when is_list(owner_principal_ids) do
    case Enum.all?(owner_principal_ids, fn value -> is_binary(value) and value != "" end) do
      true -> {:ok, Enum.uniq(owner_principal_ids)}
      false -> {:error, :forbidden}
    end
  end

  defp normalize_owner_principal_ids(_owner_principal_ids), do: {:error, :forbidden}

  defp required_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_non_empty_string(_value), do: {:error, :invalid_session}

  defp principal_type("user"), do: {:ok, :user}
  defp principal_type("agent"), do: {:ok, :agent}
  defp principal_type(:user), do: {:ok, :user}
  defp principal_type(:agent), do: {:ok, :agent}
  defp principal_type(_value), do: {:error, :invalid_session}
end
