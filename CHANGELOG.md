# Changelog

All notable changes to Spectre Beam are documented in this file.

## [Unreleased]

## [0.3.0] - 2026-08-13

### Changed

- Replaced the test-only Spectre Git dependency with
  `{:spectre, "~> 0.3.0", only: :test}` while retaining Beam's GitHub-only
  distribution.
- Aligned the package version and Stack manifest with Spectre Hex `0.3.0`
  while keeping Spectre outside Beam's runtime dependency graph.

### Fixed

- Corrected the local token-bucket reservation algorithm so callers arriving
  between already-booked slots no longer accumulate phantom rate-limit debt.
- Kept outbound idempotency claims fenced after provider success when receipt
  pipelines or store persistence fail, preventing bookkeeping failures from
  causing duplicate external messages.
- Revalidated pipeline-transformed inbound, outbound, content, and receipt
  values instead of allowing malformed structs to bypass boundary invariants.
- Made retry-filter failures provider-neutral and retryable later, accepted
  valid zero-wait throttle reservations, and fixed inclusive reply-delay
  ranges.
- Reported ExWapp delivery as acknowledged only when an acknowledgement was
  actually awaited, and preserved media payloads for normalized typed events.
- Normalized invalid action-provider resolver replies, idempotency keys, and
  identity/Instance options into stable errors instead of runtime exceptions.

## [0.2.0] - 2026-08-08

### Added

- Delivery logistics on every endpoint, applied while the outbound
  idempotency claim is held: `typing:` (provider typing indicator through the
  new optional `c:Spectre.Beam.Channel.typing/3` callback), `reply_delay_ms:`
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
- `send_await:` adapter option on the ExWapp adapter: a
  `(client, to, text, timeout, send_opts)` function the host supplies when
  its provider version has no awaited-send arity that carries send options.
  An awaited send with options and no capable provider function is a
  `{:beam_provider_callback_missing, module, :send_message_await, 5}` error —
  never a silent drop of `retry_message_id:`.

### Changed

- Removed Spectre from Beam's runtime dependency graph. The repository now
  consumes the `elchemista/spectre` GitHub `0.2.0` tag only in `MIX_ENV=test`.
- Aligned the Beam package and Stack manifest at `0.2.0`, compatible with
  Spectre `~> 0.2.0`.
- Late-bound the Stack, Agent, action-provider, identity, Instance, Turn, and
  Journal integration through Spectre's public contracts.
- Added direct configuration and delivery entry points for using the Beam
  channel boundary independently of an Agent.
- Retained the full Spectre integration, ExGram, ExWapp, pipeline,
  idempotency, and recovery test suites against Spectre GitHub `0.2.0`.
- Made distribution GitHub-only by removing Hex package metadata and package
  build CI, and pinned the test dependency to the Spectre GitHub `0.2.0` tag.

## [0.1.6] - 2026-07-31

### Changed

- Established a recoverable consolidation baseline with an explicit normative
  public API manifest and complete release documentation.
- Added no runtime functionality and made no intentional breaking API change.

[Unreleased]: https://github.com/elchemista/spectre_beam/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/elchemista/spectre_beam/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/elchemista/spectre_beam/compare/v0.1.6...v0.2.0
[0.1.6]: https://github.com/elchemista/spectre_beam/compare/v0.1.5...v0.1.6
