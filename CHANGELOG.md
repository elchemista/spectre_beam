# Changelog

All notable changes to Spectre Beam are documented in this file.

## [Unreleased]

### Added

- Delivery logistics on every endpoint, applied while the outbound
  idempotency claim is held: `typing:` (provider typing indicator through the
  new optional `Spectre.Beam.Channel.typing/3` callback), `reply_delay_ms:`
  (fixed or `{min, max}` randomized pause between the typing signal and the
  provider call), `retry:` (bounded exponential backoff for plain adapter
  errors — ambiguous outcomes are never retried), and `throttle:` (outbound
  pacing). All four accept Stack-level defaults, per-channel configuration,
  and per-call overrides.
- `Spectre.Beam.Throttle` behaviour with the bundled reservation-based
  `Spectre.Beam.Throttle.Local` pacer (endpoint token bucket + `min_delay_ms`
  spacing + per-conversation intervals + jitter, `on_limit: :wait | :error`,
  `max_wait_ms` saturation guard), started by Beam's application supervisor.
- `typing/3` implementations in the bundled ExGram and ExWapp adapters that
  call the provider's `send_typing/3` dynamically and stay best effort.
- Text deliveries now forward `send_opts` when the provider module exports
  the wider send arity (`send_message/4`, `send_message_await/5`), fixing the
  silent drop of `parse_mode`/`reply_to` options on plain text sends.

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
