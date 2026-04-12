defmodule StarciteWeb.Auth.Policy do
  @moduledoc """
  Claim-driven authorization policy for web endpoints.
  """

  alias Starcite.Auth.Principal
  alias Starcite.Session
  alias Starcite.Storage.SessionCatalog
  alias StarciteWeb.Auth.Context

  @type auth :: Context.t()
  @type session_action :: :read | :manage | :append
  @type session_resource ::
          Session.t()
          | SessionCatalog.session_entry()
          | %{required(:id) => String.t(), required(:tenant_id) => String.t()}

  @spec can_create_session(auth(), map()) :: {:ok, Principal.t()} | {:error, atom()}
  def can_create_session(%Context{kind: :none, principal: %Principal{} = principal}, _params),
    do: {:ok, principal}

  def can_create_session(
        %Context{kind: :jwt, principal: %Principal{} = principal} = auth,
        _params
      ) do
    with :ok <- authorize_scope(auth, "session:create") do
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

  @spec authorize_session_list(auth(), map()) :: {:ok, map()} | {:error, atom()}
  def authorize_session_list(%Context{} = auth, opts) when is_map(opts) do
    with :ok <- authorize_scope(auth, action_scope(:read)),
         {:ok, scoped_opts} <- scope_session_list(auth, opts) do
      {:ok, scoped_opts}
    end
  end

  def authorize_session_list(%Context{}, _opts), do: {:error, :invalid_list_query}
  def authorize_session_list(_auth, _opts), do: {:error, :forbidden}

  @spec authorize_session_access(auth(), String.t(), session_action()) :: :ok | {:error, atom()}
  def authorize_session_access(%Context{} = auth, session_id, action)
      when is_binary(session_id) and session_id != "" and action in [:read, :manage, :append] do
    with :ok <- authorize_scope(auth, action_scope(action)),
         :ok <- authorize_session_pin(auth, session_id) do
      :ok
    end
  end

  def authorize_session_access(%Context{}, _session_id, action)
      when action in [:read, :manage, :append],
      do: {:error, :invalid_session_id}

  def authorize_session_access(_auth, _session_id, _action), do: {:error, :forbidden}

  @spec authorize_session_resource(auth(), session_resource(), session_action()) ::
          :ok | {:error, atom()}
  def authorize_session_resource(%Context{kind: :none}, %{id: session_id}, action)
      when is_binary(session_id) and session_id != "" and action in [:read, :manage] do
    :ok
  end

  def authorize_session_resource(
        %Context{kind: :jwt, principal: %Principal{}} = auth,
        %{id: session_id, tenant_id: session_tenant_id},
        action
      )
      when is_binary(session_id) and session_id != "" and
             is_binary(session_tenant_id) and session_tenant_id != "" and
             action in [:read, :manage] do
    with :ok <- authorize_session_access(auth, session_id, action),
         :ok <- require_same_tenant(auth, session_tenant_id) do
      :ok
    end
  end

  def authorize_session_resource(%Context{}, _session, action)
      when action in [:read, :manage],
      do: {:error, :forbidden_session}

  def authorize_session_resource(_auth, _session, _action), do: {:error, :forbidden}

  @spec authorize_lifecycle_subscription(auth()) :: {:ok, String.t()} | {:error, atom()}
  def authorize_lifecycle_subscription(%Context{
        kind: :none,
        principal: %Principal{tenant_id: tenant_id}
      })
      when is_binary(tenant_id) and tenant_id != "" do
    {:ok, tenant_id}
  end

  def authorize_lifecycle_subscription(
        %Context{
          kind: :jwt,
          session_id: nil,
          principal: %Principal{type: :service, tenant_id: tenant_id}
        } = auth
      )
      when is_binary(tenant_id) and tenant_id != "" do
    with :ok <- authorize_scope(auth, action_scope(:read)) do
      {:ok, tenant_id}
    end
  end

  def authorize_lifecycle_subscription(%Context{kind: :jwt, session_id: session_id})
      when is_binary(session_id) and session_id != "" do
    {:error, :forbidden_session}
  end

  def authorize_lifecycle_subscription(%Context{kind: :jwt, principal: %Principal{}}),
    do: {:error, :forbidden}

  def authorize_lifecycle_subscription(_auth), do: {:error, :forbidden}

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

  defp authorize_scope(%Context{kind: :none}, _scope), do: :ok

  defp authorize_scope(%Context{kind: :jwt, principal: %Principal{}, scopes: scopes}, scope)
       when is_binary(scope) and scope != "" and is_list(scopes) do
    if Enum.member?(scopes, scope), do: :ok, else: {:error, :forbidden_scope}
  end

  defp authorize_scope(_auth, _scope), do: {:error, :forbidden_scope}

  defp scope_session_list(%Context{kind: :none}, opts) when is_map(opts), do: {:ok, opts}

  defp scope_session_list(
         %Context{
           kind: :jwt,
           principal: %Principal{tenant_id: tenant_id},
           session_id: nil
         },
         opts
       )
       when is_binary(tenant_id) and tenant_id != "" and is_map(opts) do
    {:ok, Map.put(opts, :tenant_id, tenant_id)}
  end

  defp scope_session_list(
         %Context{
           kind: :jwt,
           principal: %Principal{tenant_id: tenant_id},
           session_id: session_id
         },
         opts
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(session_id) and
              session_id != "" and is_map(opts) do
    {:ok,
     opts
     |> Map.put(:tenant_id, tenant_id)
     |> Map.put(:session_ids, [session_id])}
  end

  defp scope_session_list(_auth, _opts), do: {:error, :forbidden}

  defp authorize_session_pin(%Context{kind: :none}, session_id)
       when is_binary(session_id) and session_id != "",
       do: :ok

  defp authorize_session_pin(%Context{kind: :jwt, session_id: nil}, session_id)
       when is_binary(session_id) and session_id != "",
       do: :ok

  defp authorize_session_pin(%Context{kind: :jwt, session_id: session_id}, session_id)
       when is_binary(session_id) and session_id != "",
       do: :ok

  defp authorize_session_pin(%Context{kind: :jwt, session_id: session_id}, _session_id)
       when is_binary(session_id) and session_id != "",
       do: {:error, :forbidden_session}

  defp authorize_session_pin(_auth, _session_id), do: {:error, :forbidden_session}

  defp action_scope(:read), do: "session:read"
  defp action_scope(:manage), do: "session:create"
  defp action_scope(:append), do: "session:append"

  defp require_same_tenant(
         %Context{kind: :jwt, principal: %Principal{tenant_id: tenant_id}},
         tenant_id
       )
       when is_binary(tenant_id) and tenant_id != "",
       do: :ok

  defp require_same_tenant(
         %Context{kind: :jwt, principal: %Principal{tenant_id: expected_tenant_id}},
         tenant_id
       )
       when is_binary(expected_tenant_id) and expected_tenant_id != "" and
              is_binary(tenant_id) and tenant_id != "",
       do: {:error, :forbidden_tenant}
end
