# Tests must run serially because runtime processes and application env
# mutations are global.
ExUnit.start(max_cases: 1)

unless Code.ensure_loaded?(Starcite.Runtime.TestHelper) do
  Code.require_file("support/runtime_helper.ex", __DIR__)
end

# Application is already started by mix test with config from config/test.exs.
{:ok, _} = Application.ensure_all_started(:starcite)
:ok = Starcite.Runtime.TestHelper.reset()
