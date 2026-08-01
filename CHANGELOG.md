# Changelog

All notable changes to Spectre Beam are documented in this file.

## [Unreleased]

### Changed

- Removed Spectre from Beam's runtime dependency graph. The repository now
  consumes `elchemista/spectre` from GitHub `main` only in `MIX_ENV=test`.
- Kept the Beam package at `0.1.6`; the Stack manifest declares compatibility
  with Spectre `~> 0.2.0` without pretending Beam itself has a `0.2.0` release.
- Late-bound the Stack, Agent, action-provider, identity, Instance, Turn, and
  Journal integration through Spectre's public contracts.
- Added direct configuration and delivery entry points for using the Beam
  channel boundary independently of an Agent.
- Retained the full Spectre integration, ExGram, ExWapp, pipeline,
  idempotency, and recovery test suites against Spectre GitHub `main`.

## [0.1.6] - 2026-07-31

### Changed

- Established a recoverable consolidation baseline with an explicit normative
  public API manifest and complete release documentation.
- Added no runtime functionality and made no intentional breaking API change.

[Unreleased]: https://github.com/elchemista/spectre_beam/compare/v0.1.6...HEAD
[0.1.6]: https://github.com/elchemista/spectre_beam/compare/v0.1.5...v0.1.6
