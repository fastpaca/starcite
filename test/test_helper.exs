# Application is already started by mix test with config from config/test.exs
# Just verify it started successfully
{:ok, _} = Application.ensure_all_started(:starcite)

exclude_tags =
  case System.get_env("STARCITE_ARCHIVE_ADAPTER") do
    nil -> []
    "" -> []
    "postgres" -> []
    _other -> [requires_postgres: true]
  end

# Tests must run serially because runtime processes and application env
# mutations are global.
ExUnit.start(max_cases: 1, exclude: exclude_tags)
