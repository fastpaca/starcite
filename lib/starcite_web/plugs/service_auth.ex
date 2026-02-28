defmodule StarciteWeb.Plugs.ServiceAuth do
  @moduledoc """
  JWT authentication plug for API and WebSocket upgrade requests.

  Every authenticated request must present a bearer JWT that is verified
  against configured JWKS signing keys.
  """

  @behaviour Plug

  import Phoenix.Controller, only: [json: 2]
  import Plug.Conn

  alias Plug.Conn
  alias Starcite.Auth.Principal
  alias StarciteWeb.Auth.{Config, Context, JWT}
  alias StarciteWeb.Plugs.RedactSensitiveQuery

  @impl true
  def init(_opts), do: %{}

  @impl true
  def call(conn, _opts) do
    case authenticate_conn(conn) do
      {:ok, auth_context} ->
        assign(conn, :auth, auth_context)

      {:error, reason} ->
        unauthorized(conn, reason)
    end
  end

  @spec config() :: Config.t()
  def config do
    Config.load()
  end

  @spec mode() :: :none | :jwt
  def mode do
    config().mode
  end

  @spec authenticate_conn(Conn.t()) :: {:ok, Context.t()} | {:error, atom()}
  def authenticate_conn(%Conn{} = conn) do
    case mode() do
      :none ->
        {:ok, Context.none()}

      :jwt ->
        with {:ok, token} <- bearer_token(conn),
             {:ok, auth_context} <- authenticate_token(token) do
          {:ok, auth_context}
        end
    end
  end

  @spec authenticate_token(String.t()) :: {:ok, Context.t()} | {:error, atom()}
  def authenticate_token(token) when is_binary(token) and token != "" do
    case mode() do
      :none ->
        {:ok, Context.none()}

      :jwt ->
        cfg = config()

        with {:ok, claims} <- JWT.verify(token, cfg),
             {:ok, exp} <- claim_exp(claims),
             {:ok, tenant_id} <- claim_tenant_id(claims),
             {:ok, scopes} <- claim_scopes(claims),
             {:ok, session_id} <- claim_session_id(claims),
             {:ok, subject} <- claim_subject(claims),
             {:ok, principal} <- principal_from_subject(subject, tenant_id) do
          {:ok,
           %Context{
             kind: :jwt,
             claims: claims,
             tenant_id: tenant_id,
             scopes: scopes,
             session_id: session_id,
             subject: subject,
             principal: principal,
             expires_at: exp,
             bearer_token: token
           }}
        end
    end
  end

  def authenticate_token(_token), do: {:error, :invalid_bearer_token}

  @spec bearer_token(Conn.t()) :: {:ok, String.t()} | {:error, atom()}
  def bearer_token(%Conn{} = conn) do
    case {authorization_token(conn), websocket_query_token(conn)} do
      {{:ok, token}, :missing} ->
        {:ok, token}

      {:missing, {:ok, token}} ->
        {:ok, token}

      {{:ok, _header_token}, {:ok, _query_token}} ->
        {:error, :invalid_bearer_token}

      {{:error, reason}, _query_result} ->
        {:error, reason}

      {_header_result, {:error, reason}} ->
        {:error, reason}

      {:missing, :missing} ->
        {:error, :missing_bearer_token}
    end
  end

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

  defp claim_exp(%{"exp" => exp}) when is_integer(exp) and exp >= 0, do: {:ok, exp}
  defp claim_exp(_claims), do: {:error, :invalid_jwt_claims}

  defp claim_tenant_id(%{"tenant_id" => tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: {:ok, tenant_id}

  defp claim_tenant_id(_claims), do: {:error, :invalid_jwt_claims}

  defp claim_scopes(claims) when is_map(claims) do
    with {:ok, scope_values} <- scope_values(Map.get(claims, "scope")),
         {:ok, scopes_values} <- scopes_values(Map.get(claims, "scopes")) do
      scopes = Enum.uniq(scope_values ++ scopes_values)

      if scopes == [] do
        {:error, :invalid_jwt_claims}
      else
        {:ok, scopes}
      end
    end
  end

  defp claim_session_id(%{"session_id" => session_id})
       when is_binary(session_id) and session_id != "", do: {:ok, session_id}

  defp claim_session_id(%{"session_id" => nil}), do: {:ok, nil}
  defp claim_session_id(%{"session_id" => _invalid}), do: {:error, :invalid_jwt_claims}
  defp claim_session_id(_claims), do: {:ok, nil}

  defp claim_subject(%{"sub" => subject}) when is_binary(subject) and subject != "" do
    case String.split(subject, ":", parts: 2) do
      [principal_type, principal_id] when principal_type != "" and principal_id != "" ->
        {:ok, subject}

      _other ->
        {:error, :invalid_jwt_claims}
    end
  end

  defp claim_subject(_claims), do: {:error, :invalid_jwt_claims}

  defp principal_from_subject("user:" <> id, tenant_id)
       when is_binary(id) and id != "" and is_binary(tenant_id) and tenant_id != "" do
    Principal.new(tenant_id, id, :user)
  end

  defp principal_from_subject("agent:" <> id, tenant_id)
       when is_binary(id) and id != "" and is_binary(tenant_id) and tenant_id != "" do
    Principal.new(tenant_id, id, :agent)
  end

  defp principal_from_subject(_subject, _tenant_id), do: {:ok, nil}

  defp scope_values(nil), do: {:ok, []}

  defp scope_values(scope) when is_binary(scope) do
    scope_values = String.split(scope, ~r/\s+/, trim: true)
    {:ok, scope_values}
  end

  defp scope_values(_scope), do: {:error, :invalid_jwt_claims}

  defp scopes_values(nil), do: {:ok, []}

  defp scopes_values(scopes) when is_list(scopes) do
    normalized = Enum.uniq(scopes)

    if Enum.all?(normalized, &is_non_empty_string/1) do
      {:ok, normalized}
    else
      {:error, :invalid_jwt_claims}
    end
  end

  defp scopes_values(_scopes), do: {:error, :invalid_jwt_claims}

  defp is_non_empty_string(value), do: is_binary(value) and value != ""

  defp authorization_token(%Conn{} = conn) do
    case Conn.get_req_header(conn, "authorization") do
      [header] ->
        parse_bearer_token(header)

      [] ->
        :missing

      _many ->
        {:error, :invalid_bearer_token}
    end
  end

  defp websocket_query_token(%Conn{} = conn) do
    if websocket_upgrade_request?(conn) do
      case access_token_from_private(conn) do
        {:ok, token} ->
          {:ok, token}

        :missing ->
          parse_access_token_param(conn.query_string)

        {:error, reason} ->
          {:error, reason}
      end
    else
      :missing
    end
  end

  defp access_token_from_private(%Conn{private: private}) when is_map(private) do
    case Map.get(private, RedactSensitiveQuery.ws_access_token_private_key()) do
      nil ->
        :missing

      token when is_binary(token) ->
        parse_raw_token(token)

      _other ->
        {:error, :invalid_bearer_token}
    end
  end

  defp websocket_upgrade_request?(%Conn{} = conn) do
    conn
    |> Conn.get_req_header("upgrade")
    |> Enum.flat_map(&Plug.Conn.Utils.list/1)
    |> Enum.any?(fn value -> String.downcase(value) == "websocket" end)
  end

  defp parse_access_token_param(query_string) when is_binary(query_string) do
    case URI.decode_query(query_string) do
      %{"access_token" => token} when is_binary(token) ->
        parse_raw_token(token)

      _params ->
        :missing
    end
  rescue
    _error ->
      {:error, :invalid_bearer_token}
  end

  defp parse_bearer_token(header) when is_binary(header) do
    with [scheme, token] <- String.split(String.trim(header), " ", parts: 2, trim: true),
         true <- String.downcase(scheme) == "bearer",
         {:ok, token} <- parse_raw_token(token) do
      {:ok, token}
    else
      _ -> {:error, :invalid_bearer_token}
    end
  end

  defp parse_raw_token(token) when is_binary(token) do
    trimmed = String.trim(token)

    cond do
      trimmed == "" -> {:error, :invalid_bearer_token}
      String.contains?(trimmed, ",") -> {:error, :invalid_bearer_token}
      String.contains?(trimmed, " ") -> {:error, :invalid_bearer_token}
      true -> {:ok, trimmed}
    end
  end
end
