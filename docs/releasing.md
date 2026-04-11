# Releasing Starcite

Starcite now uses a tag-driven release flow:

- `CHANGELOG.md` is the human-readable source of truth.
- `mix.exs` carries the application version.
- `.mise.toml` pins the local Erlang and Elixir toolchain used for release prep
  and local verification.
- `scripts/version.exs` bumps `mix.exs` using semantic version rules.
- `scripts/release.exs` promotes `Unreleased`, runs `mix precommit`, commits the
  release prep, and creates the annotated tag.
- `scripts/verify_release.exs` verifies that a prepared release version matches
  both `mix.exs` and `CHANGELOG.md`.
- A `vX.Y.Z` git tag is the automation trigger.
- Tag pushes publish Docker images through
  `.github/workflows/docker-build.yml`.
- Tag pushes also create or update the matching GitHub Release from
  `CHANGELOG.md`.

## Current baseline

The repository is aligned on [`mix.exs`](../mix.exs) version `0.0.1` and the
existing `v0.0.1` tag. The next release should be chosen intentionally from
there:

- use `0.0.2` if you want to treat the current stream as another pre-1.0 patch
  release
- use `0.1.0` if you want the current accumulated API, transport, and
  operations changes to mark a minor pre-1.0 step forward

Before running release or verification commands locally:

```bash
mise install
```

Then either activate `mise` in your shell or prefix commands with `mise exec --`.

## Day-to-day rule

Update `CHANGELOG.md` in the same pull request whenever a change is user-facing
or operator-facing:

- API behavior, auth behavior, or protocol changes
- deployment and runtime configuration changes
- observability and operational behavior changes
- product-facing features such as new endpoints or channels

Skip pure refactors and internal test-only churn unless they change observable
behavior.

Keep new work under `## [Unreleased]` until release prep.

## Release steps

1. Make sure the intended release commit is checked out locally and CI is
   green.
2. Pick the next semantic version from the current `0.0.1` baseline.
3. From a clean worktree, run the release script:

   ```bash
   mise exec -- mix run --no-start scripts/release.exs patch
   ```

   Or set an explicit version:

   ```bash
   mise exec -- mix run --no-start scripts/release.exs set X.Y.Z
   ```

   The script:
   - updates `mix.exs`
   - promotes `CHANGELOG.md` from `Unreleased` to `## [X.Y.Z] - YYYY-MM-DD`
   - runs `mix precommit` which includes compile, dialyzer, format, and tests
   - runs `mise exec -- mix run --no-start scripts/verify_release.exs X.Y.Z`
   - aborts if release prep or `mix precommit` leaves any unexpected file
     changes outside `mix.exs` and `CHANGELOG.md`
   - commits `release: vX.Y.Z`
   - creates an annotated `vX.Y.Z` tag

4. Push the release commit and tag:

   ```bash
   git push origin <current-branch>
   git push origin vX.Y.Z
   ```

5. Watch GitHub Actions:
   - `Build and Push Docker Image` publishes `ghcr.io/fastpaca/starcite:X.Y.Z`
     plus the major and minor aliases.
   - `Publish GitHub Release` creates or updates the GitHub Release using the
     matching `CHANGELOG.md` section.
6. Smoke-check the published image tag and GitHub Release notes before
   announcing the release.

## Rules

- `mix.exs`, the changelog heading, and the git tag must match exactly.
- The release workflow fails if `CHANGELOG.md` does not contain a matching
  `## [X.Y.Z]` section for the pushed tag.
- `scripts/release.exs` refuses to run from a dirty worktree and aborts if the
  target tag already exists locally or on `origin`.
- `scripts/release.exs` also aborts if the prepared release fails local version
  and changelog verification or if `mix precommit` dirties files outside the
  expected release artifacts.
- Do not bump the version on ordinary feature or fix PRs. Bump once during
  release prep.
- Crossing from `0.x` to `1.0.0` is blocked by default. Set
  `STARCITE_ALLOW_MAJOR=1` when you intentionally want to cross that boundary.
- Do not force-move release tags. If a fix is needed, cut a new version.
- If a tag exists but the GitHub Release notes are wrong, update the matching
  changelog section and rerun the release workflow or edit the release in
  GitHub.

## Hotfixes

Use the same flow for hotfixes:

1. Branch from the release commit you need to patch.
2. Add the fix and update `CHANGELOG.md`.
3. Run `mise exec -- mix run --no-start scripts/release.exs patch`.
4. Push the release commit and tag, then let the workflows publish the
   artifacts.
