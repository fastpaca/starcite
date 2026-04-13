---
name: commit
description: Prepare commits in the Starcite repo. Use when the user asks to commit work, write a commit message, or finalize changes. Classify whether the diff needs a `CHANGELOG.md` entry under `Unreleased`, add it when needed, and use conventional commit messages without changing release versions or running release prep.
---

This skill handles ordinary Starcite commit prep. It does not own the release
process.

If there is any ambiguity about repo policy, read `AGENTS.md` and
`docs/releasing.md` before editing.

## Workflow

1. Inspect the diff and touched files before proposing a commit message.
2. Decide whether the change is user-facing or operator-facing.
3. If it is, add a concise entry under `## [Unreleased]` in `CHANGELOG.md`.
4. Choose the conventional commit type that best matches the primary change.
5. Run `mise exec -- mix precommit` before any actual `git commit`.
6. Only create the commit if the user explicitly asked for it.

## Changelog Rule

Update `CHANGELOG.md` in the same change when the diff affects observable
behavior, including:

- API behavior, auth behavior, or protocol changes
- deployment or runtime configuration changes
- observability or operational behavior changes
- product-facing features such as new endpoints or channels
- externally visible fixes
- measurable performance work that matters to users or operators

Skip changelog edits for:

- pure refactors with no behavior change
- internal test-only churn
- docs-only changes unless operator setup or runtime behavior changed

Keep all new entries under `## [Unreleased]`.

Do not bump `mix.exs`.
Do not create tags.
Do not run release scripts as part of commit prep.

Use existing `Unreleased` headings when possible:

- `Added` for new capabilities
- `Changed` for behavior or contract changes
- `Fixed` for bug fixes
- `Performance` for measurable latency, throughput, or resource wins

If the right heading does not exist under `Unreleased`, create it. Do not edit
historical release sections unless the user explicitly asks.

## Commit Message Rule

Use conventional commit messages:

- `feat` for new user-facing or operator-facing behavior
- `fix` for behavior corrections
- `perf` for measurable performance optimizations
- `refactor` for internal restructuring with no behavior change
- `docs` for documentation-only work
- `test` for test-only work
- `chore` for maintenance or tooling work

Use a scope only when it adds signal, for example:

- `feat(tail): add replay cursor validation`
- `fix(auth): reject mismatched tenant claims`
- `perf(append): skip redundant read path lookup`

Subject line rules:

- use the imperative mood
- do not end with a period
- keep it short, ideally 72 characters or less
- describe the primary change, not an implementation checklist

## Guardrails

- Commit messages do not determine the release version.
- Do not infer or edit semantic versions from commit types.
- Do not add a changelog entry just because a commit uses `feat`, `fix`, or
  `perf`; the change must still be observable.
- If one diff mixes unrelated changes, suggest splitting it into separate
  commits before committing.
- If the user asks only for a message, provide the message and the changelog
  recommendation without running `git commit`.

## Quick Mapping

- New endpoint, channel, or capability: changelog entry plus `feat`
- Visible bug fix: changelog entry plus `fix`
- Observable hot-path improvement: changelog entry plus `perf`
- Internal refactor with no contract change: no changelog plus `refactor`
- Docs-only change: usually no changelog plus `docs`
