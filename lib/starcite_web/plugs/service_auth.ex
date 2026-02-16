defmodule StarciteWeb.Plugs.ServiceAuth do
  @moduledoc """
  Service-level authentication plug and JWT config gateway.
  """

  @behaviour Plug

  import Phoenix.Controller, only: [json: 2]
  import Plug.Conn

  require Logger

  alias Plug.Conn
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.JWT

  @default_jwt_leeway_seconds 30
  @default_jwks_refresh_ms :timer.seconds(60)
  @default_principal_token_salt "principal-token-v1"
  @default_principal_token_default_ttl_seconds 600
  @default_principal_token_max_ttl_seconds 900

  @type mode :: :none | :jwt

  @type config :: %{
          mode: mode(),
          issuer: String.t() | nil,
          audience: String.t() | nil,
          jwks_url: String.t() | nil,
          jwt_leeway_seconds: non_neg_integer(),
          jwks_refresh_ms: pos_integer(),
          principal_token_salt: String.t(),
          principal_token_default_ttl_seconds: pos_integer(),
          principal_token_max_ttl_seconds: pos_integer()
        }

  @impl true
  def init(opts) do
    %{required: Keyword.get(opts, :required, true)}
  end

  @impl true
  def call(conn, %{required: required}) do
    case authenticate_service_conn(conn) do
      {:ok, auth_context} ->
        # Service auth wins outright in layered mode; token auth should not override it.
        assign(conn, :auth, auth_context)

      {:error, reason} ->
        if required do
          Logger.debug("Service auth rejected request: #{inspect(reason)}")
          unauthorized(conn, reason)
        else
          conn
        end
    end
  end

  @spec mode() :: mode()
  def mode do
    config().mode
  end

  @spec config() :: config()
  def config do
    Application.get_env(:starcite, StarciteWeb.Auth, [])
    |> normalize_config()
  end

  @spec require_service(Context.t()) :: :ok | {:error, :forbidden}
  def require_service(%Context{token_kind: token_kind}) when token_kind in [:none, :service],
    do: :ok

  def require_service(_auth_context), do: {:error, :forbidden}

  @spec authenticate_service_conn(Conn.t()) :: {:ok, Context.t()} | {:error, atom()}
  def authenticate_service_conn(%Conn{} = conn) do
    case config() do
      %{mode: :none} ->
        {:ok, %Context{}}

      %{mode: :jwt} = config ->
        with {:ok, token} <- bearer_token(conn),
             {:ok, claims} <- JWT.verify(token, config),
             {:ok, exp} <- claim_exp(claims) do
          {:ok,
           %Context{claims: claims, expires_at: exp, bearer_token: token, token_kind: :service}}
        end
    end
  end

  @spec authenticate_service_token(String.t()) :: {:ok, Context.t()} | {:error, atom()}
  def authenticate_service_token(token) when is_binary(token) and token != "" do
    case config() do
      %{mode: :none} ->
        {:ok, %Context{}}

      %{mode: :jwt} = config ->
        with {:ok, claims} <- JWT.verify(token, config),
             {:ok, exp} <- claim_exp(claims) do
          {:ok,
           %Context{claims: claims, expires_at: exp, bearer_token: token, token_kind: :service}}
        end
    end
  end

  def authenticate_service_token(_token), do: {:error, :invalid_bearer_token}

  @spec bearer_token(Conn.t()) :: {:ok, String.t()} | {:error, atom()}
  def bearer_token(%Conn{} = conn) do
    case Conn.get_req_header(conn, "authorization") do
      [header] ->
        parse_bearer_token(header)

      [] ->
        {:error, :missing_bearer_token}

      _many ->
        {:error, :invalid_bearer_token}
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

  defp parse_bearer_token(header) when is_binary(header) do
    with [scheme, token] <- String.split(String.trim(header), " ", parts: 2, trim: true),
         true <- String.downcase(scheme) == "bearer",
         true <- token != "",
         false <- String.contains?(token, " ") do
      {:ok, token}
    else
      _ -> {:error, :invalid_bearer_token}
    end
  end

  defp normalize_config(raw) when is_map(raw) do
    raw
    |> Map.to_list()
    |> normalize_config()
  end

  defp normalize_config(raw) when is_list(raw) do
    mode = Keyword.get(raw, :mode, :none)

    jwt_leeway_seconds =
      non_neg_integer!(
        Keyword.get(raw, :jwt_leeway_seconds),
        :jwt_leeway_seconds,
        @default_jwt_leeway_seconds
      )

    jwks_refresh_ms =
      pos_integer!(Keyword.get(raw, :jwks_refresh_ms), :jwks_refresh_ms, @default_jwks_refresh_ms)

    principal_token_salt =
      optional_non_empty_string!(
        Keyword.get(raw, :principal_token_salt),
        :principal_token_salt,
        @default_principal_token_salt
      )

    principal_token_default_ttl_seconds =
      pos_integer!(
        Keyword.get(raw, :principal_token_default_ttl_seconds),
        :principal_token_default_ttl_seconds,
        @default_principal_token_default_ttl_seconds
      )

    principal_token_max_ttl_seconds =
      pos_integer!(
        Keyword.get(raw, :principal_token_max_ttl_seconds),
        :principal_token_max_ttl_seconds,
        @default_principal_token_max_ttl_seconds
      )

    if principal_token_default_ttl_seconds > principal_token_max_ttl_seconds do
      raise ArgumentError,
            "invalid Starcite auth principal token ttl defaults: default (#{principal_token_default_ttl_seconds}) exceeds max (#{principal_token_max_ttl_seconds})"
    end

    case mode do
      :none ->
        %{
          mode: :none,
          issuer: nil,
          audience: nil,
          jwks_url: nil,
          jwt_leeway_seconds: jwt_leeway_seconds,
          jwks_refresh_ms: jwks_refresh_ms,
          principal_token_salt: principal_token_salt,
          principal_token_default_ttl_seconds: principal_token_default_ttl_seconds,
          principal_token_max_ttl_seconds: principal_token_max_ttl_seconds
        }

      :jwt ->
        %{
          mode: :jwt,
          issuer: required_string!(Keyword.get(raw, :issuer), :issuer),
          audience: required_string!(Keyword.get(raw, :audience), :audience),
          jwks_url: required_string!(Keyword.get(raw, :jwks_url), :jwks_url),
          jwt_leeway_seconds: jwt_leeway_seconds,
          jwks_refresh_ms: jwks_refresh_ms,
          principal_token_salt: principal_token_salt,
          principal_token_default_ttl_seconds: principal_token_default_ttl_seconds,
          principal_token_max_ttl_seconds: principal_token_max_ttl_seconds
        }

      other ->
        raise ArgumentError, "invalid Starcite auth mode: #{inspect(other)}"
    end
  end

  defp normalize_config(_raw) do
    normalize_config([])
  end

  defp required_string!(value, _field) when is_binary(value) and value != "", do: value

  defp required_string!(value, field) do
    raise ArgumentError, "invalid Starcite auth value for #{field}: #{inspect(value)}"
  end

  defp optional_non_empty_string!(nil, _field, default), do: default

  defp optional_non_empty_string!(value, _field, _default) when is_binary(value) and value != "",
    do: value

  defp optional_non_empty_string!(value, field, _default) do
    raise ArgumentError, "invalid Starcite auth value for #{field}: #{inspect(value)}"
  end

  defp non_neg_integer!(nil, _field, default), do: default
  defp non_neg_integer!(value, _field, _default) when is_integer(value) and value >= 0, do: value

  defp non_neg_integer!(value, field, _default) do
    raise ArgumentError, "invalid Starcite auth integer for #{field}: #{inspect(value)}"
  end

  defp pos_integer!(nil, _field, default), do: default
  defp pos_integer!(value, _field, _default) when is_integer(value) and value > 0, do: value

  defp pos_integer!(value, field, _default) do
    raise ArgumentError, "invalid Starcite auth integer for #{field}: #{inspect(value)}"
  end
end
