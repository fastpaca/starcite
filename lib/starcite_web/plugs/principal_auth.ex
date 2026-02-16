defmodule StarciteWeb.Plugs.PrincipalAuth do
  @moduledoc """
  Principal-token authentication plug plus principal boundary authorization helpers.
  """

  @behaviour Plug

  import Phoenix.Controller, only: [json: 2]
  import Plug.Conn

  require Logger

  alias Plug.Conn
  alias Starcite.Auth.Principal
  alias Starcite.Runtime
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.PrincipalToken
  alias StarciteWeb.Plugs.ServiceAuth

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Conn{assigns: %{auth: _auth_context}} = conn, _opts), do: conn

  def call(conn, _opts) do
    case authenticate_principal_conn(conn) do
      {:ok, auth_context} ->
        assign(conn, :auth, auth_context)

      {:error, reason} ->
        Logger.debug("Principal auth rejected request: #{inspect(reason)}")
        unauthorized(conn, reason)
    end
  end

  @spec authenticate_principal_conn(Conn.t()) :: {:ok, Context.t()} | {:error, atom()}
  def authenticate_principal_conn(%Conn{} = conn) do
    case ServiceAuth.mode() do
      :none ->
        {:ok, %Context{}}

      :jwt ->
        with {:ok, token} <- ServiceAuth.bearer_token(conn),
             {:ok, auth_context} <- authenticate_principal_token(token) do
          {:ok, %{auth_context | bearer_token: token}}
        end
    end
  end

  @spec authenticate_principal_token(String.t()) :: {:ok, Context.t()} | {:error, atom()}
  def authenticate_principal_token(token) when is_binary(token) and token != "" do
    case ServiceAuth.mode() do
      :none ->
        {:ok, %Context{}}

      :jwt ->
        case PrincipalToken.verify(token, ServiceAuth.config()) do
          {:ok, principal_context} ->
            {:ok,
             %Context{
               claims: principal_context.claims,
               expires_at: principal_context.expires_at,
               bearer_token: nil,
               token_kind: :principal,
               principal: principal_context.principal,
               scopes: principal_context.scopes,
               session_ids: principal_context.session_ids,
               owner_principal_ids: principal_context.owner_principal_ids
             }}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  def authenticate_principal_token(_token), do: {:error, :invalid_bearer_token}

  @spec authenticate_token(String.t()) :: {:ok, Context.t()} | {:error, atom()}
  def authenticate_token(token) when is_binary(token) and token != "" do
    case ServiceAuth.mode() do
      :none ->
        {:ok, %Context{}}

      :jwt ->
        case ServiceAuth.authenticate_service_token(token) do
          {:ok, auth_context} ->
            {:ok, auth_context}

          {:error, service_reason} ->
            case authenticate_principal_token(token) do
              {:ok, principal_context} ->
                {:ok, %{principal_context | bearer_token: token}}

              {:error, principal_reason} ->
                {:error, choose_auth_error(service_reason, principal_reason)}
            end
        end
    end
  end

  def authenticate_token(_token), do: {:error, :invalid_bearer_token}

  @spec issue_principal_token(Context.t(), PrincipalToken.issue_attrs()) ::
          {:ok, map()} | {:error, atom()}
  def issue_principal_token(
        %Context{token_kind: token_kind},
        %{
          "principal" => %{
            "tenant_id" => _tenant_id,
            "id" => _principal_id,
            "type" => _principal_type
          },
          "scopes" => _scopes
        } = attrs
      )
      when token_kind in [:none, :service] do
    with {:ok, issued} <- PrincipalToken.issue(attrs, ServiceAuth.config()) do
      principal = issued.principal

      {:ok,
       %{
         token: issued.token,
         token_type: "Bearer",
         expires_at: issued.expires_at,
         expires_in: issued.expires_in,
         principal: principal_payload(principal),
         scopes: issued.scopes,
         session_ids: issued.session_ids,
         owner_principal_ids: issued.owner_principal_ids
       }}
    end
  end

  def issue_principal_token(%Context{token_kind: token_kind}, _attrs)
      when token_kind in [:none, :service],
      do: {:error, :invalid_issue_request}

  def issue_principal_token(_auth_context, _attrs), do: {:error, :forbidden}

  @spec list_sessions_scope(Context.t()) :: {:ok, :all | [String.t()]} | {:error, :forbidden}
  def list_sessions_scope(%Context{token_kind: token_kind}) when token_kind in [:none, :service],
    do: {:ok, :all}

  def list_sessions_scope(%Context{token_kind: :principal, principal: %Principal{type: :agent}}),
    do: {:error, :forbidden}

  def list_sessions_scope(%Context{
        token_kind: :principal,
        principal: %Principal{type: :user, id: principal_id},
        owner_principal_ids: owner_principal_ids
      })
      when is_binary(principal_id) and principal_id != "" do
    with {:ok, owner_principal_ids} <- optional_principal_ids(owner_principal_ids) do
      {:ok, Enum.uniq([principal_id | owner_principal_ids])}
    end
  end

  def list_sessions_scope(%Context{token_kind: :principal}), do: {:error, :forbidden}

  @spec authorize_scope(Context.t(), String.t()) :: :ok | {:error, :forbidden_scope}
  def authorize_scope(%Context{token_kind: token_kind}, _scope)
      when token_kind in [:none, :service],
      do: :ok

  def authorize_scope(%Context{token_kind: :principal, scopes: scopes}, scope)
      when is_binary(scope) and scope != "" and is_list(scopes) do
    if Enum.member?(scopes, scope), do: :ok, else: {:error, :forbidden_scope}
  end

  def authorize_scope(_auth_context, _scope), do: {:error, :forbidden_scope}

  @spec authorize_session(Context.t(), String.t()) :: :ok | {:error, :forbidden_session}
  def authorize_session(%Context{token_kind: token_kind}, _session_id)
      when token_kind in [:none, :service],
      do: :ok

  def authorize_session(
        %Context{token_kind: :principal, principal: %Principal{type: :agent}, session_ids: nil},
        _session_id
      ),
      do: {:error, :forbidden_session}

  def authorize_session(
        %Context{token_kind: :principal, principal: %Principal{type: :user}, session_ids: nil},
        session_id
      )
      when is_binary(session_id) and session_id != "",
      do: :ok

  def authorize_session(%Context{token_kind: :principal, session_ids: session_ids}, session_id)
      when is_binary(session_id) and session_id != "" and is_list(session_ids) do
    if Enum.member?(session_ids, session_id), do: :ok, else: {:error, :forbidden_session}
  end

  def authorize_session(_auth_context, _session_id), do: {:error, :forbidden_session}

  @spec authorize_tenant(Context.t(), map()) :: :ok | {:error, :forbidden_tenant}
  def authorize_tenant(%Context{token_kind: token_kind}, _resource)
      when token_kind in [:none, :service],
      do: :ok

  def authorize_tenant(
        %Context{token_kind: :principal, principal: %Principal{tenant_id: tenant_id}},
        session
      )
      when is_binary(tenant_id) and tenant_id != "" and is_map(session) do
    with {:ok, creator_principal} <- session_creator_principal(session) do
      if creator_principal.tenant_id == tenant_id do
        :ok
      else
        {:error, :forbidden_tenant}
      end
    else
      {:error, _reason} -> {:error, :forbidden_tenant}
    end
  end

  def authorize_tenant(%Context{token_kind: :principal}, _resource),
    do: {:error, :forbidden_tenant}

  @spec authorize_principal_session_access(Context.t(), map(), String.t()) ::
          :ok | {:error, :forbidden_session}
  def authorize_principal_session_access(%Context{token_kind: token_kind}, _session, _scope)
      when token_kind in [:none, :service],
      do: :ok

  def authorize_principal_session_access(
        %Context{token_kind: :principal, principal: %Principal{type: :agent}},
        _session,
        _scope
      ),
      do: :ok

  def authorize_principal_session_access(
        %Context{
          token_kind: :principal,
          principal: %Principal{type: :user},
          session_ids: session_ids
        },
        _session,
        _scope
      )
      when is_list(session_ids),
      do: :ok

  def authorize_principal_session_access(
        %Context{
          token_kind: :principal,
          principal: %Principal{type: :user} = principal,
          session_ids: nil,
          owner_principal_ids: owner_principal_ids
        },
        session,
        _scope
      )
      when is_map(session) do
    with {:ok, creator_principal} <- session_creator_principal(session),
         {:ok, owner_principal_ids} <- optional_principal_ids(owner_principal_ids) do
      self_owner? =
        creator_principal.type == principal.type and creator_principal.id == principal.id

      policy_owner? = Enum.member?(owner_principal_ids, creator_principal.id)

      if self_owner? or policy_owner? do
        :ok
      else
        {:error, :forbidden_session}
      end
    end
  end

  def authorize_principal_session_access(%Context{token_kind: :principal}, _session, _scope),
    do: {:error, :forbidden_session}

  @spec authorize_session_request(Context.t(), String.t(), String.t()) ::
          {:ok, Starcite.Session.t()} | {:error, atom()}
  def authorize_session_request(%Context{} = auth_context, session_id, scope)
      when is_binary(session_id) and session_id != "" and is_binary(scope) and scope != "" do
    with :ok <- authorize_scope(auth_context, scope),
         :ok <- authorize_session(auth_context, session_id),
         {:ok, session} <- Runtime.get_session(session_id),
         :ok <- authorize_tenant(auth_context, session),
         :ok <- authorize_principal_session_access(auth_context, session, scope) do
      {:ok, session}
    end
  end

  def authorize_session_request(_auth_context, _session_id, _scope),
    do: {:error, :invalid_session_id}

  @spec resolve_actor(Context.t(), String.t() | nil) ::
          {:ok, String.t()} | {:error, :invalid_event}
  def resolve_actor(%Context{token_kind: :principal, principal: %Principal{} = principal}, nil) do
    {:ok, Principal.actor(principal)}
  end

  def resolve_actor(
        %Context{token_kind: :principal, principal: %Principal{} = principal},
        requested_actor
      )
      when is_binary(requested_actor) and requested_actor != "" do
    actor = Principal.actor(principal)
    if requested_actor == actor, do: {:ok, actor}, else: {:error, :invalid_event}
  end

  def resolve_actor(%Context{token_kind: :principal}, _requested_actor),
    do: {:error, :invalid_event}

  def resolve_actor(_auth_context, actor) when is_binary(actor) and actor != "", do: {:ok, actor}
  def resolve_actor(_auth_context, _actor), do: {:error, :invalid_event}

  @spec stamp_event_metadata(Context.t(), map()) :: map()
  def stamp_event_metadata(
        %Context{token_kind: :principal, principal: principal},
        metadata
      )
      when is_map(metadata) do
    principal_metadata = principal_metadata(principal)

    Map.update(metadata, "starcite_principal", principal_metadata, fn
      existing when is_map(existing) -> Map.merge(existing, principal_metadata)
      _other -> principal_metadata
    end)
  end

  def stamp_event_metadata(_auth_context, metadata) when is_map(metadata), do: metadata

  defp unauthorized(conn, reason) do
    conn
    |> put_resp_header("www-authenticate", unauthorized_header(reason))
    |> put_status(:unauthorized)
    |> json(%{error: "unauthorized", message: "Unauthorized"})
    |> halt()
  end

  defp unauthorized_header(:missing_bearer_token) do
    ~s(Bearer realm="starcite")
  end

  defp unauthorized_header(:invalid_bearer_token) do
    ~s(Bearer realm="starcite", error="invalid_request", error_description="Malformed bearer token")
  end

  defp unauthorized_header(_reason) do
    ~s(Bearer realm="starcite", error="invalid_token")
  end

  defp choose_auth_error(:token_expired, _principal_reason), do: :token_expired
  defp choose_auth_error(_jwt_reason, :token_expired), do: :token_expired
  defp choose_auth_error(jwt_reason, _principal_reason), do: jwt_reason

  defp session_creator_principal(%{creator_principal: %Principal{} = principal}),
    do: {:ok, principal}

  defp session_creator_principal(%{creator_principal: creator_principal})
       when is_map(creator_principal) do
    with {:ok, tenant_id} <-
           required_string(creator_principal["tenant_id"] || creator_principal[:tenant_id]),
         {:ok, principal_id} <- required_string(creator_principal["id"] || creator_principal[:id]),
         {:ok, principal_type} <-
           principal_type(creator_principal["type"] || creator_principal[:type]),
         {:ok, principal} <- Principal.new(tenant_id, principal_id, principal_type) do
      {:ok, principal}
    else
      {:error, _reason} -> {:error, :forbidden_session}
    end
  end

  defp session_creator_principal(_session), do: {:error, :forbidden_session}

  defp required_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_string(_value), do: {:error, :invalid_principal}

  defp principal_type("user"), do: {:ok, :user}
  defp principal_type("agent"), do: {:ok, :agent}
  defp principal_type(:user), do: {:ok, :user}
  defp principal_type(:agent), do: {:ok, :agent}
  defp principal_type(_value), do: {:error, :invalid_principal}

  defp optional_principal_ids(nil), do: {:ok, []}

  defp optional_principal_ids(principal_ids) when is_list(principal_ids) do
    case Enum.all?(principal_ids, fn value -> is_binary(value) and value != "" end) do
      true -> {:ok, Enum.uniq(principal_ids)}
      false -> {:error, :forbidden}
    end
  end

  defp optional_principal_ids(_principal_ids), do: {:error, :forbidden}

  defp principal_payload(%Principal{} = principal) do
    %{
      tenant_id: principal.tenant_id,
      id: principal.id,
      type: Atom.to_string(principal.type)
    }
  end

  defp principal_metadata(%Principal{} = principal) do
    %{
      "tenant_id" => principal.tenant_id,
      "principal_type" => Atom.to_string(principal.type),
      "principal_id" => principal.id,
      "actor" => Principal.actor(principal)
    }
  end
end
