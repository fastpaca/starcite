defmodule FleetLM.Agent.TestPlug do
  @moduledoc """
  Test plug for agent webhooks. Intercepts requests and sends them to the test process.
  """

  @behaviour Plug

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    payload = Jason.decode!(body)

    test_pid =
      case Application.get_env(:fleet_lm, :agent_test_pid) do
        nil -> raise "No test PID configured"
        pid -> pid
      end

    send(test_pid, {:agent_webhook_called, payload, self()})

    response_data =
      receive do
        {:agent_response, data} -> data
      after
        5_000 -> %{"kind" => "text", "content" => %{"text" => "timeout"}}
      end

    jsonl_line = Jason.encode!(response_data) <> "\n"

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(200, jsonl_line)
  end
end
