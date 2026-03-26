defmodule StarciteWeb.CORS do
  @moduledoc false

  # Phoenix websocket upgrades do not run through CORSPlug. When HTTP CORS is
  # fully permissive, we must also disable Phoenix's separate websocket origin
  # check or browser upgrades with an Origin header get a 403 before our socket
  # code runs.
  def websocket_check_origin(opts) when is_list(opts) do
    case Keyword.fetch!(opts, :origin) do
      "*" -> false
      origin when is_binary(origin) -> [origin]
      origins when is_list(origins) -> origins
    end
  end
end
