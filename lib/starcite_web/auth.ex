defmodule StarciteWeb.Auth do
  @moduledoc """
  Request authentication for Starcite's API boundary.

  Supported modes:

  - `:none` - API is unauthenticated
  - `:jwt` - bearer JWT validated against JWKS, issuer, audience, and expiry
  """

  alias Plug.Conn
  alias StarciteWeb.Auth.JWT

  @default_jwt_leeway_seconds 30
  @default_jwks_refresh_ms :timer.minutes(5)

  @type mode :: :none | :jwt

  @type config :: %{
          mode: mode(),
          issuer: String.t() | nil,
          audience: String.t() | nil,
          jwks_url: String.t() | nil,
          jwt_leeway_seconds: non_neg_integer(),
          jwks_refresh_ms: pos_integer()
        }

  @type auth_context :: %{
          claims: map(),
          expires_at: pos_integer() | nil,
          bearer_token: String.t() | nil
        }

  @spec mode() :: mode()
  def mode do
    config().mode
  end

  @spec authenticate_conn(Conn.t()) :: {:ok, auth_context()} | {:error, atom()}
  def authenticate_conn(%Conn{} = conn) do
    case config() do
      %{mode: :none} ->
        {:ok, %{claims: %{}, expires_at: nil, bearer_token: nil}}

      %{mode: :jwt} = config ->
        authenticate_jwt(conn, config)
    end
  end

  @spec authenticate_token(String.t()) :: {:ok, auth_context()} | {:error, atom()}
  def authenticate_token(token) when is_binary(token) and token != "" do
    case config() do
      %{mode: :none} ->
        {:ok, %{claims: %{}, expires_at: nil, bearer_token: nil}}

      %{mode: :jwt} = config ->
        authenticate_jwt_token(token, config)
    end
  end

  def authenticate_token(_token), do: {:error, :invalid_bearer_token}

  @spec config() :: config()
  def config do
    Application.get_env(:starcite, __MODULE__, [])
    |> normalize_config()
  end

  defp authenticate_jwt(conn, config) do
    with {:ok, token} <- bearer_token(conn),
         {:ok, auth_context} <- authenticate_jwt_token(token, config) do
      {:ok, auth_context}
    end
  end

  defp authenticate_jwt_token(token, config) when is_binary(token) and is_map(config) do
    with {:ok, claims} <- JWT.verify(token, config),
         {:ok, exp} <- claim_exp(claims) do
      {:ok, %{claims: claims, expires_at: exp, bearer_token: token}}
    end
  end

  defp claim_exp(%{"exp" => exp}) when is_integer(exp) and exp >= 0, do: {:ok, exp}
  defp claim_exp(_claims), do: {:error, :invalid_jwt_claims}

  defp bearer_token(conn) do
    case Conn.get_req_header(conn, "authorization") do
      [header] ->
        parse_bearer_token(header)

      [] ->
        {:error, :missing_bearer_token}

      _many ->
        {:error, :invalid_bearer_token}
    end
  end

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
      non_neg_integer!(Keyword.get(raw, :jwt_leeway_seconds), :jwt_leeway_seconds)

    jwks_refresh_ms = pos_integer!(Keyword.get(raw, :jwks_refresh_ms), :jwks_refresh_ms)

    case mode do
      :none ->
        %{
          mode: :none,
          issuer: nil,
          audience: nil,
          jwks_url: nil,
          jwt_leeway_seconds: jwt_leeway_seconds,
          jwks_refresh_ms: jwks_refresh_ms
        }

      :jwt ->
        %{
          mode: :jwt,
          issuer: required_string!(Keyword.get(raw, :issuer), :issuer),
          audience: required_string!(Keyword.get(raw, :audience), :audience),
          jwks_url: required_string!(Keyword.get(raw, :jwks_url), :jwks_url),
          jwt_leeway_seconds: jwt_leeway_seconds,
          jwks_refresh_ms: jwks_refresh_ms
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

  defp non_neg_integer!(nil, _field), do: @default_jwt_leeway_seconds
  defp non_neg_integer!(value, _field) when is_integer(value) and value >= 0, do: value

  defp non_neg_integer!(value, field) do
    raise ArgumentError, "invalid Starcite auth integer for #{field}: #{inspect(value)}"
  end

  defp pos_integer!(nil, _field), do: @default_jwks_refresh_ms
  defp pos_integer!(value, _field) when is_integer(value) and value > 0, do: value

  defp pos_integer!(value, field) do
    raise ArgumentError, "invalid Starcite auth integer for #{field}: #{inspect(value)}"
  end
end
