defmodule Starcite.WritePath do
  @moduledoc """
  Write path for session creation and append/ack operations.
  """

  alias Phoenix.PubSub
  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.{SessionQuorum, SessionStore}
  alias Starcite.Observability.Telemetry
  alias Starcite.Routing.SessionRouter
  alias Starcite.Session
  alias Starcite.Session.Header
  alias Starcite.Storage.SessionCatalog

  @spec create_session(keyword()) :: {:ok, map()} | {:error, term()}
  def create_session(opts \\ []) when is_list(opts) do
    id =
      case Keyword.get(opts, :id) do
        nil -> generate_session_id()
        value -> value
      end

    title = Keyword.get(opts, :title)
    metadata = Keyword.get(opts, :metadata, %{})

    if is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and is_map(metadata) do
      dispatch_create_session(
        id,
        title,
        Keyword.get(opts, :creator_principal),
        Keyword.get(opts, :tenant_id),
        metadata
      )
    else
      {:error, :invalid_session}
    end
  end

  @doc false
  @spec create_session_local(String.t(), String.t() | nil, Principal.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def create_session_local(id, title, creator_principal, tenant_id, metadata)
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
             is_map(metadata) do
    do_create_session_local(id, title, creator_principal, tenant_id, metadata)
  end

  def create_session_local(_id, _title, _creator_principal, _tenant_id, _metadata),
    do: {:error, :invalid_session}

  defp dispatch_create_session(
         id,
         title,
         %Principal{tenant_id: tenant_id} = creator_principal,
         tenant_id,
         metadata
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    route_create_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp dispatch_create_session(
         id,
         title,
         %Principal{tenant_id: tenant_id} = creator_principal,
         nil,
         metadata
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    route_create_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp dispatch_create_session(id, title, nil, tenant_id, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_binary(tenant_id) and tenant_id != "" and is_map(metadata) do
    {:ok, creator_principal} = Principal.new(tenant_id, "service", :service)
    route_create_session(id, title, creator_principal, tenant_id, metadata)
  end

  defp dispatch_create_session(id, title, nil, nil, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) do
    {:ok, creator_principal} = Principal.new("service", "service", :service)
    route_create_session(id, title, creator_principal, "service", metadata)
  end

  defp dispatch_create_session(_id, _title, _creator_principal, _tenant_id, _metadata),
    do: {:error, :invalid_session}

  defp route_create_session(id, title, creator_principal, tenant_id, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
              is_map(metadata) do
    route_to_replica(
      id,
      :create_session_local,
      [id, title, creator_principal, tenant_id, metadata],
      :create_session_local,
      [id, title, creator_principal, tenant_id, metadata]
    )
  end

  defp do_create_session_local(id, title, creator_principal, tenant_id, metadata)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
              is_map(metadata) do
    with :ok <- SessionRouter.ensure_local_owner(id) do
      header =
        Header.new(id,
          title: title,
          creator_principal: creator_principal,
          tenant_id: tenant_id,
          metadata: metadata
        )

      session =
        header
        |> Session.new_from_header()
        |> assign_routing_epoch()

      case SessionQuorum.start_session(session) do
        :ok ->
          case SessionQuorum.replicate_state(session, []) do
            :ok ->
              :ok = SessionStore.put_session(session)
              session_map = session |> Session.to_map(header) |> Map.put(:archived, false)

              :ok =
                PubSub.broadcast(
                  Starcite.PubSub,
                  "lifecycle:" <> session.tenant_id,
                  {:session_lifecycle,
                   %{
                     kind: "session.created",
                     session_id: session.id,
                     tenant_id: session.tenant_id,
                     title: session_map.title,
                     metadata: session_map.metadata,
                     created_at: session_map.created_at,
                     version: session_map.version
                   }}
                )

              _ = maybe_index_session(session_map, creator_principal, tenant_id)
              {:ok, session_map}

            {:error, _reason} = error ->
              cleanup_uncommitted_session(session.id)
              error
          end

        {:error, :session_exists} ->
          {:error, :session_exists}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp cleanup_uncommitted_session(session_id)
       when is_binary(session_id) and session_id != "" do
    :ok = SessionQuorum.stop_session(session_id)
    :ok = SessionStore.delete_session(session_id)
    :ok
  end

  defp assign_routing_epoch(%Session{id: session_id, epoch: current_epoch} = session)
       when is_binary(session_id) and session_id != "" and is_integer(current_epoch) and
              current_epoch >= 0 do
    epoch = SessionRouter.local_owner_epoch(session_id, current_epoch)
    %Session{session | epoch: epoch}
  end

  @spec update_session(String.t(), map()) :: {:ok, map()} | {:error, term()} | {:timeout, term()}
  def update_session(id, attrs) when is_binary(id) and id != "" and is_map(attrs) do
    route_to_replica(
      id,
      :update_session_local,
      [id, attrs],
      :update_session_local,
      [id, attrs]
    )
  end

  def update_session(_id, _attrs), do: {:error, :invalid_session}

  @doc false
  @spec update_session_local(String.t(), map()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def update_session_local(id, attrs) when is_binary(id) and id != "" and is_map(attrs) do
    with {:ok, session} <- SessionQuorum.get_session(id),
         {:ok, header} <- SessionCatalog.update_header(id, attrs) do
      session_map = Session.to_map(session, header)
      :ok = publish_session_updated(session_map, session.tenant_id)
      {:ok, session_map}
    end
  end

  def update_session_local(_id, _attrs), do: {:error, :invalid_session}

  @spec append_event(String.t(), map()) ::
          {:ok, %{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}}
          | {:error, term()}
          | {:timeout, term()}
  def append_event(id, event) when is_binary(id) and id != "" and is_map(event) do
    route_to_replica(
      id,
      :append_event_local,
      [id, event, []],
      :append_event_local,
      [id, event, []]
    )
  end

  def append_event(_id, _event), do: {:error, :invalid_event}

  @spec append_event(String.t(), map(), keyword()) ::
          {:ok, %{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}}
          | {:error, term()}
          | {:timeout, term()}
  def append_event(id, event, opts)
      when is_binary(id) and id != "" and is_map(event) and is_list(opts) do
    route_to_replica(
      id,
      :append_event_local,
      [id, event, opts],
      :append_event_local,
      [id, event, opts]
    )
  end

  def append_event(_id, _event, _opts), do: {:error, :invalid_event}

  @doc false
  @spec append_event_local(String.t(), map()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def append_event_local(id, event)
      when is_binary(id) and id != "" and is_map(event) do
    measure_ack_request(:append_event, fn ->
      SessionQuorum.append_event(id, event, nil)
    end)
  end

  @doc false
  @spec append_event_local(String.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def append_event_local(id, event, opts) when is_binary(id) and id != "" and is_map(event) do
    expected_seq = expected_seq_from_opts(opts)

    measure_ack_request(:append_event, fn ->
      SessionQuorum.append_event(id, event, expected_seq)
    end)
  end

  @spec append_events(String.t(), [map()], keyword()) ::
          {:ok,
           %{
             results: [%{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}],
             last_seq: non_neg_integer()
           }}
          | {:error, term()}
          | {:timeout, term()}
  def append_events(id, events, opts \\ [])

  def append_events(id, events, opts)
      when is_binary(id) and id != "" and is_list(events) and events != [] and is_list(opts) do
    route_to_replica(
      id,
      :append_events_local,
      [id, events, opts],
      :append_events_local,
      [id, events, opts]
    )
  end

  def append_events(_id, _events, _opts), do: {:error, :invalid_event}

  @doc false
  @spec append_events_local(String.t(), [map()], keyword()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def append_events_local(id, events, opts \\ [])
      when is_binary(id) and id != "" and is_list(events) and events != [] and is_list(opts) do
    measure_ack_request(:append_events, fn ->
      SessionQuorum.append_events(id, events, opts)
    end)
  end

  @spec put_projection_items(String.t(), [map()]) ::
          {:ok, [map()]} | {:error, term()} | {:timeout, term()}
  def put_projection_items(id, items)
      when is_binary(id) and id != "" and is_list(items) and items != [] do
    route_to_replica(
      id,
      :put_projection_items_local,
      [id, items],
      :put_projection_items_local,
      [id, items]
    )
  end

  def put_projection_items(_id, _items), do: {:error, :invalid_projection_item}

  @doc false
  @spec put_projection_items_local(String.t(), [map()]) ::
          {:ok, [map()]} | {:error, term()} | {:timeout, term()}
  def put_projection_items_local(id, items)
      when is_binary(id) and id != "" and is_list(items) and items != [] do
    SessionQuorum.put_projection_items(id, items)
  end

  def put_projection_items_local(_id, _items), do: {:error, :invalid_projection_item}

  @spec delete_projection_item(String.t(), String.t()) ::
          :ok | {:error, term()} | {:timeout, term()}
  def delete_projection_item(id, item_id)
      when is_binary(id) and id != "" and is_binary(item_id) and item_id != "" do
    route_to_replica(
      id,
      :delete_projection_item_local,
      [id, item_id],
      :delete_projection_item_local,
      [id, item_id]
    )
  end

  def delete_projection_item(_id, _item_id), do: {:error, :invalid_projection_item}

  @doc false
  @spec delete_projection_item_local(String.t(), String.t()) ::
          :ok | {:error, term()} | {:timeout, term()}
  def delete_projection_item_local(id, item_id)
      when is_binary(id) and id != "" and is_binary(item_id) and item_id != "" do
    SessionQuorum.delete_projection_item(id, item_id)
  end

  def delete_projection_item_local(_id, _item_id), do: {:error, :invalid_projection_item}

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def ack_archived(id, upto_seq) when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    route_to_replica(
      id,
      :ack_archived_local,
      [id, upto_seq],
      :ack_archived_local,
      [id, upto_seq]
    )
  end

  @doc false
  @spec ack_archived_local(String.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def ack_archived_local(id, upto_seq)
      when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    SessionQuorum.ack_archived(id, upto_seq)
  end

  @spec archive_session(String.t()) :: {:ok, map()} | {:error, term()}
  def archive_session(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, %{session: session, changed: changed?}} <-
           SessionCatalog.archive_session(session_id) do
      if changed?, do: :ok = broadcast_archive_lifecycle(session, "session.archived")
      {:ok, session}
    end
  end

  def archive_session(_session_id), do: {:error, :invalid_session_id}

  @spec unarchive_session(String.t()) :: {:ok, map()} | {:error, term()}
  def unarchive_session(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, %{session: session, changed: changed?}} <-
           SessionCatalog.unarchive_session(session_id) do
      if changed?, do: :ok = broadcast_archive_lifecycle(session, "session.unarchived")
      {:ok, session}
    end
  end

  def unarchive_session(_session_id), do: {:error, :invalid_session_id}

  defp route_to_replica(session_id, remote_fun, remote_args, local_fun, local_args)
       when is_binary(session_id) and session_id != "" and is_atom(remote_fun) and
              is_list(remote_args) and is_atom(local_fun) and is_list(local_args) do
    SessionRouter.call(
      session_id,
      __MODULE__,
      remote_fun,
      remote_args,
      __MODULE__,
      local_fun,
      local_args,
      prefer_leader: true,
      request_operation: request_operation(local_fun)
    )
  end

  defp maybe_index_session(
         %{
           id: id,
           title: title,
           metadata: metadata,
           created_at: created_at
         },
         creator_principal,
         tenant_id
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_struct(creator_principal, Principal) and is_binary(tenant_id) and tenant_id != "" and
              is_map(metadata) do
    header =
      Header.new(id,
        title: title,
        creator_principal: creator_principal,
        tenant_id: tenant_id,
        metadata: metadata,
        timestamp: parse_naive_datetime!(created_at)
      )

    case SessionCatalog.persist_created(header) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp maybe_index_session(_session, _creator_principal, _tenant_id), do: :ok

  defp publish_session_updated(
         %{
           id: id,
           title: title,
           metadata: metadata,
           updated_at: updated_at,
           version: version
         },
         tenant_id
       )
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) and is_binary(updated_at) and updated_at != "" and
              is_integer(version) and version > 0 and
              is_binary(tenant_id) and tenant_id != "" do
    PubSub.broadcast(
      Starcite.PubSub,
      "lifecycle:" <> tenant_id,
      {:session_lifecycle,
       %{
         kind: "session.updated",
         session_id: id,
         tenant_id: tenant_id,
         title: title,
         metadata: metadata,
         updated_at: updated_at,
         version: version
       }}
    )

    :ok
  end

  defp publish_session_updated(_session, _tenant_id), do: :ok

  defp broadcast_archive_lifecycle(
         %{id: session_id, tenant_id: tenant_id, archived: archived},
         kind
       )
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and is_boolean(archived) and is_binary(kind) and kind != "" do
    PubSub.broadcast(
      Starcite.PubSub,
      "lifecycle:" <> tenant_id,
      {:session_lifecycle,
       %{
         kind: kind,
         session_id: session_id,
         tenant_id: tenant_id,
         archived: archived
       }}
    )

    :ok
  end

  defp expected_seq_from_opts(opts) when is_list(opts) do
    Keyword.get(opts, :expected_seq)
  end

  defp parse_naive_datetime!(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> DateTime.to_naive(datetime)
      {:error, reason} -> raise ArgumentError, "invalid datetime: #{inspect(reason)}"
    end
  end

  defp generate_session_id do
    "ses_" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
  end

  defp request_operation(:append_event_local), do: :append_event
  defp request_operation(:append_events_local), do: :append_events
  defp request_operation(_local_fun), do: nil

  defp measure_ack_request(operation, fun)
       when operation in [:append_event, :append_events] and is_function(fun, 0) do
    started_at = System.monotonic_time()
    result = fun.()
    duration_ms = elapsed_ms_since(started_at)
    :ok = Telemetry.request_result(operation, :ack, result, duration_ms)
    result
  end

  defp elapsed_ms_since(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end
end
