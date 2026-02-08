# Application is already started by mix test with config from config/test.exs
# Just verify it started successfully
{:ok, _} = Application.ensure_all_started(:starcite)

# Tests must run serially because runtime processes and application env
# mutations are global.
ExUnit.start(max_cases: 1)
