defmodule Starcite.Pprof.Router do
  @moduledoc false

  use Plug.Router
  use Plug.ErrorHandler

  plug(Plug.Parsers, parsers: [:urlencoded, :multipart], pass: ["text/*"])
  plug(:match)
  plug(:dispatch)

  get "/pprof" do
    send_resp(conn, 200, "Welcome")
  end

  get "/debug/pprof/profile" do
    pid = conn.params["pid"]
    type = conn.params["type"]

    with {:ok, duration_ms} <- parse_duration_ms(conn.params["seconds"]),
         timeout_ms <- profile_timeout_ms(duration_ms),
         :ok <- collect_profile(type, pid, duration_ms, timeout_ms) do
      conn
      |> put_resp_header("Content-Disposition", "attachment; filename=profile.pb.gz")
      |> put_resp_header("Content-Type", "application/octet-stream")
      |> put_resp_header("X-Content-Type-Options", "nosniff")
      |> send_resp(200, build_profile(duration_ms))
    else
      {:error, {:bad_request, reason}} ->
        send_resp(conn, 400, reason)

      {:error, reason} ->
        send_resp(conn, 500, reason)
    end
  end

  match _ do
    send_resp(conn, 404, "Oops!")
  end

  @impl Plug.ErrorHandler
  def handle_errors(conn, %{kind: _kind, reason: _reason, stack: _stack}) do
    send_resp(conn, conn.status, "Something went wrong")
  end

  defp parse_duration_ms(nil), do: {:ok, 3000}

  defp parse_duration_ms(raw) do
    case Integer.parse(String.trim(raw)) do
      {seconds, ""} when seconds > 0 ->
        {:ok, seconds * 1000}

      _ ->
        {:error,
         {:bad_request, "invalid seconds value: #{inspect(raw)} (expected positive integer)"}}
    end
  end

  defp profile_timeout_ms(duration_ms) do
    configured_timeout = Application.get_env(:starcite, :pprof_profile_timeout_ms, 60_000)

    case configured_timeout do
      value when is_integer(value) and value > 0 ->
        max(value, duration_ms + 1_000)

      value ->
        raise ArgumentError,
              "invalid value for :pprof_profile_timeout_ms: #{inspect(value)} (expected positive integer)"
    end
  end

  defp collect_profile(type, pid, duration_ms, timeout_ms) do
    reply =
      try do
        GenServer.call(Pprof.Servers.Profile, {:profile, type, pid, duration_ms}, timeout_ms)
      catch
        :exit, {:timeout, _} ->
          {:error, "profile request timed out after #{timeout_ms}ms"}

        :exit, reason ->
          {:error, "profile request failed: #{inspect(reason)}"}
      end

    case reply do
      {:ok, _} ->
        :ok

      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp build_profile(duration_ms) do
    :erlang.apply(Pprof.Builder.Build, :builder, [duration_ms])
  end
end
