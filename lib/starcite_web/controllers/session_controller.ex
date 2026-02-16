defmodule StarciteWeb.SessionController do
  @moduledoc """
  HTTP controller for Starcite session primitives:

  - `create` via `POST /v1/sessions`
  - `append` via `POST /v1/sessions/:id/append`
  - `index` via `GET /v1/sessions`
  """

  use StarciteWeb, :controller

  alias Starcite.Auth.Principal
  alias Starcite.Runtime
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Plugs.PrincipalAuth

  action_fallback StarciteWeb.FallbackController

  @default_list_limit 100
  @max_list_limit 1000

  @doc """
  Create a session.
  """
  def create(conn, params) do
    auth_context = conn.assigns[:auth] || %Context{}

    with {:ok, opts} <- validate_create(params, auth_context),
         {:ok, session} <- Runtime.create_session(opts) do
      conn
      |> put_status(:created)
      |> json(session)
    end
  end

  @doc """
  Append one event to a session.
  """
  def append(conn, %{"id" => id} = params) do
    auth_context = conn.assigns[:auth] || %Context{}

    with {:ok, _session} <-
           PrincipalAuth.authorize_session_request(auth_context, id, "session:append"),
         {:ok, event, expected_seq} <- validate_append(params, auth_context),
         {:ok, reply} <- Runtime.append_event(id, event, expected_seq: expected_seq) do
      conn
      |> put_status(:created)
      |> json(reply)
    end
  end

  def append(_conn, _params), do: {:error, :invalid_event}

  @doc """
  List known sessions from the configured archive adapter.
  """
  def index(conn, params) do
    auth_context = conn.assigns[:auth] || %Context{}

    with {:ok, opts} <- validate_list(params),
         :ok <- PrincipalAuth.authorize_scope(auth_context, "session:read"),
         {:ok, scope} <- PrincipalAuth.list_sessions_scope(auth_context),
         {:ok, page} <- list_sessions(scope, auth_context, opts) do
      json(conn, page)
    end
  end

  # Validation

  defp validate_create(params, %Context{token_kind: token_kind})
       when token_kind in [:none, :service] do
    validate_service_create(params)
  end

  defp validate_create(
         params,
         %Context{token_kind: :principal, principal: %Principal{type: :user} = principal} =
           auth_context
       )
       when is_map(params) do
    with :ok <- PrincipalAuth.authorize_scope(auth_context, "session:create"),
         {:ok, id} <- optional_non_empty_string(params["id"]),
         {:ok, title} <- optional_string(params["title"]),
         :ok <- reject_creator_override(params["creator_principal"]),
         {:ok, metadata} <- optional_object(params["metadata"]) do
      {:ok,
       [
         id: id,
         title: title,
         creator_principal: principal,
         metadata: metadata
       ]}
    end
  end

  defp validate_create(
         _params,
         %Context{token_kind: :principal, principal: %Principal{type: :agent}}
       ),
       do: {:error, :forbidden}

  defp validate_create(_params, _auth_context), do: {:error, :invalid_session}

  defp validate_service_create(
         %{
           "creator_principal" => %{
             "tenant_id" => _tenant_id,
             "id" => _principal_id,
             "type" => _principal_type
           }
         } = params
       ) do
    with {:ok, id} <- optional_non_empty_string(params["id"]),
         {:ok, title} <- optional_string(params["title"]),
         {:ok, creator_principal} <- principal_from_payload(params["creator_principal"]),
         {:ok, metadata} <- optional_object(params["metadata"]) do
      {:ok,
       [
         id: id,
         title: title,
         creator_principal: creator_principal,
         metadata: metadata
       ]}
    end
  end

  defp validate_service_create(_params), do: {:error, :invalid_session}

  defp validate_append(
         %{
           "type" => type,
           "payload" => payload,
           "producer_id" => producer_id,
           "producer_seq" => producer_seq
         } = params,
         auth_context
       )
       when is_binary(type) and type != "" and is_map(payload) and is_map(auth_context) do
    with {:ok, validated_producer_id} <- required_non_empty_string(producer_id),
         {:ok, validated_producer_seq} <- required_positive_integer(producer_seq),
         {:ok, actor} <- PrincipalAuth.resolve_actor(auth_context, params["actor"]),
         {:ok, source} <- optional_non_empty_string(params["source"]),
         {:ok, metadata} <- optional_object(params["metadata"]),
         {:ok, refs} <- optional_refs(params["refs"]),
         {:ok, idempotency_key} <- optional_non_empty_string(params["idempotency_key"]),
         {:ok, expected_seq} <- optional_non_neg_integer(params["expected_seq"]) do
      metadata = PrincipalAuth.stamp_event_metadata(auth_context, metadata)

      event = %{
        type: type,
        payload: payload,
        actor: actor,
        source: source,
        metadata: metadata,
        refs: refs,
        idempotency_key: idempotency_key,
        producer_id: validated_producer_id,
        producer_seq: validated_producer_seq
      }

      {:ok, event, expected_seq}
    end
  end

  defp validate_append(_params, _auth_context), do: {:error, :invalid_event}

  defp validate_list(params) when is_map(params) do
    with {:ok, limit} <- optional_limit(params["limit"]),
         {:ok, cursor} <- optional_cursor(params["cursor"]),
         {:ok, metadata} <- optional_metadata_filters(params) do
      {:ok, %{limit: limit, cursor: cursor, metadata: metadata}}
    end
  end

  defp validate_list(_params), do: {:error, :invalid_list_query}

  defp list_sessions(:all, _auth_context, opts) when is_map(opts) do
    Starcite.Archive.Store.list_sessions(opts)
  end

  defp list_sessions(
         owner_principal_ids,
         %Context{token_kind: :principal, principal: %Principal{tenant_id: tenant_id}},
         opts
       )
       when is_list(owner_principal_ids) and is_binary(tenant_id) and tenant_id != "" and
              is_map(opts) do
    Starcite.Archive.Store.list_sessions(
      opts
      |> Map.put(:owner_principal_ids, owner_principal_ids)
      |> Map.put(:tenant_id, tenant_id)
    )
  end

  defp list_sessions(_scope, _auth_context, _opts), do: {:error, :forbidden}

  defp principal_from_payload(%{
         "tenant_id" => tenant_id,
         "id" => principal_id,
         "type" => principal_type
       }) do
    with {:ok, tenant_id} <- required_non_empty_string(tenant_id, :invalid_session),
         {:ok, principal_id} <- required_non_empty_string(principal_id, :invalid_session),
         {:ok, principal_type} <- principal_type(principal_type, :invalid_session),
         {:ok, principal} <- Principal.new(tenant_id, principal_id, principal_type) do
      {:ok, principal}
    else
      {:error, _reason} -> {:error, :invalid_session}
    end
  end

  defp principal_from_payload(_payload), do: {:error, :invalid_session}

  defp reject_creator_override(nil), do: :ok
  defp reject_creator_override(_creator_override), do: {:error, :invalid_session}

  defp required_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_non_empty_string(_value), do: {:error, :invalid_event}

  defp required_non_empty_string(value, _error_reason) when is_binary(value) and value != "",
    do: {:ok, value}

  defp required_non_empty_string(_value, error_reason), do: {:error, error_reason}

  defp required_positive_integer(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp required_positive_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> {:ok, parsed}
      _ -> {:error, :invalid_event}
    end
  end

  defp required_positive_integer(_value), do: {:error, :invalid_event}

  defp optional_non_empty_string(nil), do: {:ok, nil}
  defp optional_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp optional_non_empty_string(_value), do: {:error, :invalid_event}

  defp optional_object(nil), do: {:ok, %{}}
  defp optional_object(value) when is_map(value) and not is_list(value), do: {:ok, value}
  defp optional_object(_value), do: {:error, :invalid_metadata}

  defp principal_type("user", _error_reason), do: {:ok, :user}
  defp principal_type("agent", _error_reason), do: {:ok, :agent}
  defp principal_type(:user, _error_reason), do: {:ok, :user}
  defp principal_type(:agent, _error_reason), do: {:ok, :agent}
  defp principal_type(_value, error_reason), do: {:error, error_reason}

  defp optional_refs(nil), do: {:ok, %{}}

  defp optional_refs(refs) when is_map(refs) and not is_list(refs) do
    with {:ok, _} <- optional_non_neg_integer(refs["to_seq"]),
         {:ok, _} <- optional_string(refs["request_id"]),
         {:ok, _} <- optional_string(refs["sequence_id"]),
         {:ok, _} <- optional_non_neg_integer(refs["step"]) do
      {:ok, refs}
    end
  end

  defp optional_refs(_value), do: {:error, :invalid_refs}

  defp optional_string(nil), do: {:ok, nil}
  defp optional_string(value) when is_binary(value), do: {:ok, value}
  defp optional_string(_value), do: {:error, :invalid_event}

  defp optional_non_neg_integer(nil), do: {:ok, nil}
  defp optional_non_neg_integer(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp optional_non_neg_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, :invalid_event}
    end
  end

  defp optional_non_neg_integer(_value), do: {:error, :invalid_event}

  defp optional_limit(nil), do: {:ok, @default_list_limit}

  defp optional_limit(value) when is_integer(value) and value > 0 and value <= @max_list_limit,
    do: {:ok, value}

  defp optional_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 and parsed <= @max_list_limit -> {:ok, parsed}
      _ -> {:error, :invalid_limit}
    end
  end

  defp optional_limit(_value), do: {:error, :invalid_limit}

  defp optional_cursor(nil), do: {:ok, nil}
  defp optional_cursor(value) when is_binary(value) and value != "", do: {:ok, value}
  defp optional_cursor(_value), do: {:error, :invalid_cursor}

  defp optional_metadata_filters(params) when is_map(params) do
    with {:ok, nested} <- optional_metadata_filter_map(params["metadata"]) do
      dotted =
        Enum.reduce(params, %{}, fn
          {<<"metadata.", key::binary>>, value}, acc when key != "" ->
            case metadata_filter_value(value) do
              {:ok, normalized} -> Map.put(acc, key, normalized)
              :error -> acc
            end

          _, acc ->
            acc
        end)

      {:ok, Map.merge(nested, dotted)}
    end
  end

  defp optional_metadata_filter_map(nil), do: {:ok, %{}}

  defp optional_metadata_filter_map(value) when is_map(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn
      {key, entry}, {:ok, acc} when is_binary(key) and key != "" ->
        case metadata_filter_value(entry) do
          {:ok, normalized} -> {:cont, {:ok, Map.put(acc, key, normalized)}}
          :error -> {:halt, {:error, :invalid_metadata}}
        end

      _, _ ->
        {:halt, {:error, :invalid_metadata}}
    end)
  end

  defp optional_metadata_filter_map(_value), do: {:error, :invalid_metadata}

  defp metadata_filter_value(value)
       when is_binary(value) or is_boolean(value) or is_number(value),
       do: {:ok, value}

  defp metadata_filter_value(_value), do: :error
end
