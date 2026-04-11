defmodule Starcite.Session.View do
  @moduledoc """
  Session read-model helpers for raw and composed stream views.
  """

  alias Starcite.Session
  alias Starcite.Session.Projections

  @type raw_fetcher :: (pos_integer(), pos_integer() -> {:ok, [map()]} | {:error, term()})
  @type view :: :raw | :composed

  @spec replay_snapshot(Session.t()) :: %{
          required(:epoch) => non_neg_integer(),
          required(:last_seq) => non_neg_integer(),
          required(:committed_seq) => non_neg_integer()
        }
  def replay_snapshot(%Session{} = session) do
    %{
      epoch: session.epoch,
      last_seq: session.last_seq,
      committed_seq: session.archived_seq
    }
  end

  @spec from_cursor(Session.t(), non_neg_integer(), pos_integer(), view(), raw_fetcher()) ::
          {:ok, [map()]} | {:error, term()}
  def from_cursor(%Session{} = session, cursor, limit, view, fetch_raw_events)
      when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit > 0 and
             view in [:raw, :composed] and is_function(fetch_raw_events, 2) do
    case view do
      :raw -> raw_from_cursor(session, cursor, limit, fetch_raw_events)
      :composed -> composed_from_cursor(session, cursor, limit, fetch_raw_events)
    end
  end

  @spec raw_from_cursor(Session.t(), non_neg_integer(), pos_integer(), raw_fetcher()) ::
          {:ok, [map()]} | {:error, term()}
  def raw_from_cursor(%Session{} = session, cursor, limit, fetch_raw_events)
      when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit > 0 and
             is_function(fetch_raw_events, 2) do
    next_seq = cursor + 1

    cond do
      next_seq > session.last_seq ->
        {:ok, []}

      true ->
        fetch_raw_events.(next_seq, min(session.last_seq, cursor + limit))
    end
  end

  @spec composed_from_cursor(Session.t(), non_neg_integer(), pos_integer(), raw_fetcher()) ::
          {:ok, [map()]} | {:error, term()}
  def composed_from_cursor(%Session{} = session, cursor, limit, fetch_raw_events)
      when is_integer(cursor) and cursor >= 0 and is_integer(limit) and limit > 0 and
             is_function(fetch_raw_events, 2) do
    items = Projections.latest_items(session.projections)

    with plan <- build_composed_plan(cursor + 1, limit, session.last_seq, items, []),
         {:ok, raw_events_by_seq} <- fetch_plan_raw_events(plan, fetch_raw_events),
         {:ok, events} <- render_composed_plan(plan, raw_events_by_seq, []) do
      {:ok, events}
    end
  end

  @spec to_stream_item(Projections.item()) :: map()
  def to_stream_item(
        %{
          item_id: item_id,
          version: version,
          from_seq: from_seq,
          to_seq: to_seq,
          payload: payload,
          metadata: metadata,
          inserted_at: inserted_at
        } = item
      )
      when is_binary(item_id) and item_id != "" and is_integer(version) and version > 0 and
             is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq and
             is_map(payload) and is_map(metadata) and is_binary(inserted_at) do
    %{
      seq: to_seq,
      from_seq: from_seq,
      type: "projection",
      item_id: item_id,
      version: version,
      schema: Map.get(item, :schema),
      content_type: Map.get(item, :content_type),
      payload: payload,
      metadata: metadata,
      source: "projection",
      inserted_at: inserted_at
    }
  end

  defp build_composed_plan(next_seq, _remaining, last_seq, _items, acc)
       when is_integer(next_seq) and is_integer(last_seq) and next_seq > last_seq do
    Enum.reverse(acc)
  end

  defp build_composed_plan(_next_seq, 0, _last_seq, _items, acc),
    do: Enum.reverse(acc)

  defp build_composed_plan(next_seq, remaining, last_seq, items, acc)
       when is_integer(next_seq) and next_seq > 0 and is_integer(remaining) and remaining > 0 and
              is_integer(last_seq) and last_seq >= 0 and is_list(items) and is_list(acc) do
    {next_seq, items} = skip_covered_items(items, next_seq)

    cond do
      next_seq > last_seq ->
        Enum.reverse(acc)

      items == [] ->
        raw_to = min(last_seq, next_seq + remaining - 1)
        Enum.reverse([{:raw, next_seq, raw_to} | acc])

      true ->
        [%{from_seq: from_seq, to_seq: to_seq} = item | rest] = items

        if from_seq == next_seq do
          build_composed_plan(
            to_seq + 1,
            remaining - 1,
            last_seq,
            rest,
            [{:projection, item} | acc]
          )
        else
          raw_limit = min(remaining, from_seq - next_seq)
          raw_to = next_seq + raw_limit - 1

          build_composed_plan(
            raw_to + 1,
            remaining - raw_limit,
            last_seq,
            items,
            [{:raw, next_seq, raw_to} | acc]
          )
        end
    end
  end

  defp fetch_plan_raw_events(plan, _fetch_raw_events) when is_list(plan) and plan == [],
    do: {:ok, %{}}

  defp fetch_plan_raw_events(plan, fetch_raw_events)
       when is_list(plan) and is_function(fetch_raw_events, 2) do
    raw_ranges =
      Enum.flat_map(plan, fn
        {:raw, from_seq, to_seq} -> [{from_seq, to_seq}]
        _other -> []
      end)

    case raw_ranges do
      [] ->
        {:ok, %{}}

      _ ->
        {from_seq, _to_seq} = Enum.min_by(raw_ranges, &elem(&1, 0))
        {_from_seq, to_seq} = Enum.max_by(raw_ranges, &elem(&1, 1))

        case fetch_raw_events.(from_seq, to_seq) do
          {:ok, events} -> {:ok, Map.new(events, fn event -> {event.seq, event} end)}
          {:error, _reason} = error -> error
        end
    end
  end

  defp render_composed_plan([], _raw_events_by_seq, acc), do: {:ok, Enum.reverse(acc)}

  defp render_composed_plan([{:projection, item} | rest], raw_events_by_seq, acc)
       when is_map(item) and is_map(raw_events_by_seq) and is_list(acc) do
    render_composed_plan(rest, raw_events_by_seq, [to_stream_item(item) | acc])
  end

  defp render_composed_plan([{:raw, from_seq, to_seq} | rest], raw_events_by_seq, acc)
       when is_integer(from_seq) and is_integer(to_seq) and is_map(raw_events_by_seq) and
              is_list(acc) do
    case fetch_raw_range(raw_events_by_seq, from_seq, to_seq, acc) do
      {:ok, next_acc} -> render_composed_plan(rest, raw_events_by_seq, next_acc)
      {:error, _reason} = error -> error
    end
  end

  defp fetch_raw_range(raw_events_by_seq, from_seq, to_seq, acc)
       when is_map(raw_events_by_seq) and is_integer(from_seq) and is_integer(to_seq) and
              from_seq <= to_seq and is_list(acc) do
    from_seq..to_seq
    |> Enum.reduce_while({:ok, acc}, fn seq, {:ok, inner_acc} ->
      case Map.fetch(raw_events_by_seq, seq) do
        {:ok, event} -> {:cont, {:ok, [event | inner_acc]}}
        :error -> {:halt, {:error, :event_gap_detected}}
      end
    end)
  end

  defp skip_covered_items(items, next_seq)
       when is_list(items) and is_integer(next_seq) and next_seq > 0 do
    do_skip_covered_items(items, next_seq)
  end

  defp do_skip_covered_items([], next_seq), do: {next_seq, []}

  defp do_skip_covered_items([%{to_seq: to_seq} | rest], next_seq)
       when is_integer(to_seq) and to_seq < next_seq do
    do_skip_covered_items(rest, next_seq)
  end

  defp do_skip_covered_items([%{from_seq: from_seq, to_seq: to_seq} | rest], next_seq)
       when is_integer(from_seq) and is_integer(to_seq) and from_seq < next_seq and
              next_seq <= to_seq do
    do_skip_covered_items(rest, to_seq + 1)
  end

  defp do_skip_covered_items(items, next_seq), do: {next_seq, items}
end
