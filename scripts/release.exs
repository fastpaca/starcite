#!/usr/bin/env elixir

Code.require_file("release_utils.exs", __DIR__)

args = System.argv()

{type, explicit_version} =
  case args do
    [bump_type] when bump_type in ["patch", "minor", "major"] ->
      {bump_type, nil}

    ["set", version] ->
      {"set", version}

    _ ->
      Starcite.ReleaseScripts.usage!(
        "Usage: mix run --no-start scripts/release.exs <patch|minor|major|set> [version]"
      )
  end

if not Starcite.ReleaseScripts.clean_worktree?() do
  Starcite.ReleaseScripts.abort!("Working tree is not clean. Commit or stash changes first.")
end

current_version = Starcite.ReleaseScripts.current_version()
next_version = Starcite.ReleaseScripts.next_version!(type, explicit_version)
tag = "v#{next_version}"
branch = Starcite.ReleaseScripts.current_branch()
release_paths = ["CHANGELOG.md", "mix.exs"]

Starcite.ReleaseScripts.verify_release_target!(next_version)

Starcite.ReleaseScripts.write_version!(next_version)
Starcite.ReleaseScripts.promote_unreleased!(next_version)
Starcite.ReleaseScripts.verify_prepared_release!(next_version)
Starcite.ReleaseScripts.ensure_only_expected_changes!(release_paths)

IO.puts("Prepared release version: #{current_version} -> #{next_version}")

Starcite.ReleaseScripts.run!("mix", ["precommit"])

Starcite.ReleaseScripts.run!("mix", [
  "run",
  "--no-start",
  "scripts/verify_release.exs",
  next_version
])

Starcite.ReleaseScripts.ensure_only_expected_changes!(release_paths)
Starcite.ReleaseScripts.run!("git", ["add" | release_paths])
Starcite.ReleaseScripts.run!("git", ["commit", "-m", "release: #{tag}"])
Starcite.ReleaseScripts.run!("git", ["tag", "-a", tag, "-m", "Release #{tag}"])

IO.puts("")
IO.puts("Release prep complete for #{tag}.")
IO.puts("Next steps:")
IO.puts("1. Review the release commit and tag locally")
IO.puts("2. git push origin #{branch}")
IO.puts("3. git push origin #{tag}")
