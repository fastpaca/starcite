defmodule StarciteWeb.Auth.JWKSRefresher do
  @moduledoc false

  use GenServer

  alias StarciteWeb.Auth.JWKS

  @retry_ms 1_000

  def start_link(_arg) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def fetch(config, kid) when is_map(config) and is_binary(kid) and kid != "" do
    GenServer.call(__MODULE__, {:fetch, config, kid}, 5_000)
  end

  def refresh_async(config) when is_map(config) do
    GenServer.cast(__MODULE__, {:refresh_async, config})
  end

  def clear do
    GenServer.call(__MODULE__, :clear)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(:clear, _from, state) do
    Enum.each(state, fn {_url, entry} ->
      cancel_timer(entry.timer_ref)
    end)

    {:reply, :ok, %{}}
  end

  def handle_call({:fetch, config, kid}, from, state) do
    url = config.jwks_url
    entry = Map.get(state, url, new_entry(config))

    next_entry =
      if entry.in_flight do
        %{entry | waiters: [{from, kid} | entry.waiters], config: config}
      else
        entry
        |> Map.put(:config, config)
        |> Map.put(:waiters, [{from, kid} | entry.waiters])
        |> start_refresh()
      end

    {:noreply, Map.put(state, url, next_entry)}
  end

  @impl true
  def handle_cast({:refresh_async, config}, state) do
    url = config.jwks_url
    entry = Map.get(state, url, new_entry(config))

    next_entry =
      if entry.in_flight do
        %{entry | config: config}
      else
        entry
        |> Map.put(:config, config)
        |> start_refresh()
      end

    {:noreply, Map.put(state, url, next_entry)}
  end

  @impl true
  def handle_info({:refresh_complete, url, result}, state) do
    case Map.get(state, url) do
      nil ->
        {:noreply, state}

      entry ->
        Enum.each(entry.waiters, fn {from, kid} ->
          GenServer.reply(from, reply_for_fetch(entry.config, kid, result))
        end)

        next_entry = %{
          entry
          | in_flight: false,
            waiters: [],
            timer_ref: schedule_refresh(url, entry.config, result)
        }

        {:noreply, Map.put(state, url, next_entry)}
    end
  end

  def handle_info({:refresh_timer, url}, state) do
    case Map.get(state, url) do
      nil ->
        {:noreply, state}

      %{in_flight: true} ->
        {:noreply, state}

      entry ->
        {:noreply, Map.put(state, url, start_refresh(entry))}
    end
  end

  defp new_entry(config) do
    %{config: config, in_flight: false, waiters: [], timer_ref: nil}
  end

  defp start_refresh(entry) do
    cancel_timer(entry.timer_ref)
    pid = self()
    config = entry.config
    url = config.jwks_url

    Task.start(fn ->
      send(pid, {:refresh_complete, url, JWKS.refresh_url(config)})
    end)

    %{entry | in_flight: true, timer_ref: nil}
  end

  defp reply_for_fetch(config, kid, :ok) do
    case JWKS.cached_signing_key(config, kid) do
      {:ok, _freshness, signing_key} -> {:ok, signing_key}
      :miss -> {:error, :unknown_jwt_kid}
    end
  end

  defp reply_for_fetch(_config, _kid, {:error, reason}), do: {:error, reason}

  defp schedule_refresh(url, %{jwks_refresh_ms: refresh_ms}, :ok)
       when is_binary(url) and url != "" and is_integer(refresh_ms) and refresh_ms > 0 do
    Process.send_after(self(), {:refresh_timer, url}, refresh_ms)
  end

  defp schedule_refresh(url, %{jwks_refresh_ms: refresh_ms}, {:error, _reason})
       when is_binary(url) and url != "" and is_integer(refresh_ms) and refresh_ms > 0 do
    Process.send_after(self(), {:refresh_timer, url}, min(refresh_ms, @retry_ms))
  end

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(timer_ref) do
    Process.cancel_timer(timer_ref)
    :ok
  end
end
