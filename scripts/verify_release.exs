#!/usr/bin/env elixir

Code.require_file("release_utils.exs", __DIR__)

case System.argv() do
  [version] ->
    Starcite.ReleaseScripts.verify_prepared_release!(version)
    IO.puts("Prepared release #{version} passed local release checks.")

  _ ->
    Starcite.ReleaseScripts.usage!(
      "Usage: mix run --no-start scripts/verify_release.exs <version>"
    )
end
