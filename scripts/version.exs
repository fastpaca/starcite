#!/usr/bin/env elixir

Code.require_file("release_utils.exs", __DIR__)

args = System.argv()

case args do
  [type] when type in ["patch", "minor", "major"] ->
    current_version = Starcite.ReleaseScripts.current_version()
    next_version = Starcite.ReleaseScripts.next_version!(type)
    Starcite.ReleaseScripts.write_version!(next_version)
    IO.puts("Version bumped: #{current_version} -> #{next_version}")

  ["set", explicit_version] ->
    current_version = Starcite.ReleaseScripts.current_version()
    next_version = Starcite.ReleaseScripts.next_version!("set", explicit_version)
    Starcite.ReleaseScripts.write_version!(next_version)
    IO.puts("Version bumped: #{current_version} -> #{next_version}")

  _ ->
    Starcite.ReleaseScripts.usage!(
      "Usage: mix run --no-start scripts/version.exs <patch|minor|major|set> [version]"
    )
end
