# Releasing Starcite

Starcite now uses a tag-driven release flow:

- The changelog is the human-readable source of truth.
- The application version, changelog, and tag stay aligned.
- The repo uses the pinned local Erlang and Elixir toolchain for release prep
  and local verification.
- Release preparation and verification are scripted and run locally before the
  tag is pushed.
- A `vX.Y.Z` git tag is the automation trigger.
- Tag pushes publish Docker images through the configured CI workflow.
- Tag pushes also create or update the matching GitHub Release from
  the changelog.

## Current baseline

The repository is currently aligned on version `0.0.4` and tag `v0.0.4`. The
next release should be chosen intentionally from there:

- use `0.0.5` if you want to treat the current stream as another pre-1.0 patch
  release
- use `0.1.0` if you want the current accumulated API, transport, and
  operations changes to mark a minor pre-1.0 step forward

Before running release or verification commands locally:

```bash
mise install
```

Then either activate `mise` in your shell or prefix commands with `mise exec --`.

## Day-to-day rule

Update the changelog in the same pull request whenever a change is user-facing
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
2. Pick the next semantic version from the current `0.0.4` baseline.
3. From a clean worktree, run the local release-preparation command for either
   a patch bump or an explicit version set.

   The command:
   - updates the application version
   - promotes the changelog from `Unreleased` to `## [X.Y.Z] - YYYY-MM-DD`
   - runs `mix precommit` which includes compile, dialyzer, format, and tests
   - runs the release verification step for the chosen version
   - aborts if release prep or `mix precommit` leaves any unexpected file
     changes outside the expected release artifacts
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
     matching changelog section.
6. Smoke-check the published image tag and GitHub Release notes before
   announcing the release.

## Rules

- The application version, changelog heading, and git tag must match exactly.
- The release workflow fails if the changelog does not contain a matching
  `## [X.Y.Z]` section for the pushed tag.
- The release command refuses to run from a dirty worktree and aborts if the
  target tag already exists locally or on `origin`.
- The release command also aborts if the prepared release fails local version
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
2. Add the fix and update the changelog.
3. Run the patch release-preparation command.
4. Push the release commit and tag, then let the workflows publish the
   artifacts.
