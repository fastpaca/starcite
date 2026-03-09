defmodule StarciteWeb.TailAccess do
  @moduledoc false

  alias Starcite.ReadPath
  alias Starcite.Session
  alias StarciteWeb.Auth.{Context, Policy}

  @spec authorize_read(Context.t(), String.t()) :: {:ok, Session.t()} | {:error, atom()}
  def authorize_read(%Context{} = auth, session_id)
      when is_binary(session_id) and session_id != "" do
    authorize(auth, session_id, &Policy.allowed_to_read_session/2)
  end

  def authorize_read(_auth, _session_id), do: {:error, :invalid_session_id}

  @spec authorize_append(Context.t(), String.t()) :: {:ok, Session.t()} | {:error, atom()}
  def authorize_append(%Context{} = auth, session_id)
      when is_binary(session_id) and session_id != "" do
    authorize(auth, session_id, &Policy.allowed_to_append_session/2)
  end

  def authorize_append(_auth, _session_id), do: {:error, :invalid_session_id}

  defp authorize(%Context{} = auth, session_id, authorize_session)
       when is_binary(session_id) and session_id != "" and is_function(authorize_session, 2) do
    with :ok <- Policy.allowed_to_access_session(auth, session_id),
         {:ok, %Session{} = session} <- ReadPath.get_session(session_id),
         :ok <- authorize_session.(auth, session) do
      {:ok, session}
    end
  end
end
