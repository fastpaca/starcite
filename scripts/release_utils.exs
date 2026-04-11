defmodule Starcite.ReleaseScripts do
  @mix_exs_path Path.expand("../mix.exs", __DIR__)
  @changelog_path Path.expand("../CHANGELOG.md", __DIR__)
  @semver_regex ~r/^\d+\.\d+\.\d+$/
  @major_bump_override_env "STARCITE_ALLOW_MAJOR"

  def current_version do
    mix_exs = File.read!(@mix_exs_path)

    case Regex.run(~r/version:\s*"(\d+\.\d+\.\d+)"/, mix_exs, capture: :all_but_first) do
      [version] -> version
      _ -> raise "Could not find version in mix.exs"
    end
  end

  def next_version!(type, explicit_version \\ nil) do
    current_version = current_version()

    next_version =
      case type do
        "set" -> explicit_version
        "patch" -> bump(current_version, :patch)
        "minor" -> bump(current_version, :minor)
        "major" -> bump(current_version, :major)
        _ -> raise "Unsupported bump type '#{type}'"
      end

    validate_semver!(next_version)
    ensure_version_advances!(current_version, next_version)
    guard_pre_1_major!(current_version, next_version)
    next_version
  end

  def write_version!(version) do
    validate_semver!(version)
    mix_exs = File.read!(@mix_exs_path)

    updated =
      Regex.replace(
        ~r/version:\s*"\d+\.\d+\.\d+"/,
        mix_exs,
        ~s(version: "#{version}"),
        global: false
      )

    if updated == mix_exs do
      raise "Could not update version in mix.exs"
    end

    File.write!(@mix_exs_path, updated)
  end

  def promote_unreleased!(version, date \\ Date.utc_today()) do
    if changelog_has_section?(version) do
      raise "CHANGELOG.md already has a section for #{version}"
    end

    changelog = File.read!(@changelog_path)

    case Regex.named_captures(
           ~r/\A(?<before>.*?^## \[Unreleased\]\n\n)(?<body>.*?)(?<after>^## \[[^\n]+\].*)\z/ms,
           changelog
         ) do
      %{"before" => before, "body" => body, "after" => remainder} ->
        releasable_body =
          case :binary.match(body, "### ") do
            {index, _length} ->
              body
              |> binary_part(index, byte_size(body) - index)
              |> String.trim()

            :nomatch ->
              raise "CHANGELOG.md Unreleased section has no releasable entries"
          end

        if releasable_body == "" do
          raise "CHANGELOG.md Unreleased section has no releasable entries"
        end

        updated =
          before <>
            "## [#{version}] - #{Date.to_iso8601(date)}\n\n" <>
            releasable_body <>
            "\n\n" <> String.trim_leading(remainder, "\n")

        File.write!(@changelog_path, updated)

      _ ->
        raise "Could not parse CHANGELOG.md Unreleased section"
    end
  end

  def changelog_has_section?(version) do
    changelog = File.read!(@changelog_path)
    Regex.match?(~r/^## \[#{Regex.escape(version)}\](?: - .+)?$/m, changelog)
  end

  def unreleased_has_entries? do
    changelog = File.read!(@changelog_path)

    case Regex.named_captures(
           ~r/^## \[Unreleased\]\n\n(?<body>.*?)(?=^## \[[^\n]+\]|\z)/ms,
           changelog
         ) do
      %{"body" => body} ->
        String.contains?(body, "### ")

      _ ->
        false
    end
  end

  def verify_release_target!(version) do
    validate_semver!(version)
    tag = "v#{version}"

    if changelog_has_section?(version) do
      raise "CHANGELOG.md already has a section for #{version}"
    end

    if not unreleased_has_entries?() do
      raise "CHANGELOG.md Unreleased section has no releasable entries"
    end

    if local_tag_exists?(tag) do
      raise "Tag #{tag} already exists locally"
    end

    if remote_tag_exists?(tag) do
      raise "Tag #{tag} already exists on origin"
    end
  end

  def verify_prepared_release!(version) do
    validate_semver!(version)

    if current_version() != version do
      raise "mix.exs version #{current_version()} does not match prepared release #{version}"
    end

    if not changelog_has_section?(version) do
      raise "CHANGELOG.md does not contain a section for #{version}"
    end
  end

  def changed_paths do
    tracked =
      capture!("git", ["diff", "--name-only", "--relative", "HEAD"])
      |> lines()

    untracked =
      capture!("git", ["ls-files", "--others", "--exclude-standard"])
      |> lines()

    (tracked ++ untracked)
    |> Enum.uniq()
    |> Enum.sort()
  end

  def ensure_only_expected_changes!(expected_paths) do
    expected = Enum.sort(expected_paths)
    actual = changed_paths()

    if actual != expected do
      raise """
      Release flow produced unexpected working tree changes.
      Expected: #{Enum.join(expected, ", ")}
      Actual: #{Enum.join(actual, ", ")}
      """
    end
  end

  def clean_worktree? do
    capture!("git", ["status", "--porcelain"]) == ""
  end

  def local_tag_exists?(tag) do
    capture!("git", ["tag", "-l", tag]) == tag
  end

  def current_branch do
    capture!("git", ["rev-parse", "--abbrev-ref", "HEAD"])
  end

  def remote_tag_exists?(tag) do
    capture!("git", ["ls-remote", "--tags", "origin", "refs/tags/#{tag}"]) != ""
  end

  def run!(command, args, opts \\ []) do
    printed_command = Enum.join([command | args], " ")
    IO.puts("$ #{printed_command}")

    {_, exit_status} =
      System.cmd(
        command,
        args,
        Keyword.merge([into: IO.stream(:stdio, :line), stderr_to_stdout: true], opts)
      )

    if exit_status != 0 do
      System.halt(exit_status)
    end
  end

  def capture!(command, args, opts \\ []) do
    {output, exit_status} =
      System.cmd(command, args, Keyword.merge([stderr_to_stdout: true], opts))

    if exit_status != 0 do
      raise "Command failed: #{Enum.join([command | args], " ")}"
    end

    String.trim(output)
  end

  def usage!(message) do
    IO.puts(:stderr, message)
    System.halt(1)
  end

  def abort!(message) do
    IO.puts(:stderr, message)
    System.halt(1)
  end

  defp validate_semver!(version) do
    if is_binary(version) and Regex.match?(@semver_regex, version) do
      :ok
    else
      raise "Invalid version '#{inspect(version)}'"
    end
  end

  defp bump(version, type) do
    [major, minor, patch] = parse_version!(version)

    case type do
      :patch -> "#{major}.#{minor}.#{patch + 1}"
      :minor -> "#{major}.#{minor + 1}.0"
      :major -> "#{major + 1}.0.0"
    end
  end

  defp parse_version!(version) do
    version
    |> String.split(".")
    |> Enum.map(fn segment ->
      case Integer.parse(segment) do
        {value, ""} -> value
        _ -> raise "Invalid semver '#{version}'"
      end
    end)
    |> case do
      [major, minor, patch] -> [major, minor, patch]
      _ -> raise "Invalid semver '#{version}'"
    end
  end

  defp guard_pre_1_major!(current_version, next_version) do
    [current_major | _] = parse_version!(current_version)
    [next_major | _] = parse_version!(next_version)

    allow_major = System.get_env(@major_bump_override_env) == "1"

    if current_major == 0 and next_major >= 1 and not allow_major do
      raise [
              "Major bump to >=1.0.0 is blocked by default while pre-1.0.0.",
              "Use patch/minor for ongoing pre-1.0.0 releases.",
              "Set #{@major_bump_override_env}=1 to intentionally allow crossing to #{next_version}."
            ]
            |> Enum.join(" ")
    end
  end

  defp ensure_version_advances!(current_version, next_version) do
    case compare_versions(current_version, next_version) do
      :lt ->
        :ok

      :eq ->
        raise "Target version #{next_version} matches the current version"

      :gt ->
        raise "Target version #{next_version} is lower than the current version #{current_version}"
    end
  end

  defp compare_versions(left, right) do
    left_parts = parse_version!(left)
    right_parts = parse_version!(right)

    cond do
      left_parts < right_parts -> :lt
      left_parts > right_parts -> :gt
      true -> :eq
    end
  end

  defp lines(""), do: []
  defp lines(output), do: String.split(output, "\n", trim: true)
end
