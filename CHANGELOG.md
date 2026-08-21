# Changelog

All notable changes to Spectre Beam are documented in this file.

## [Unreleased]

### Added

- **Zero-configuration local chat.** Direct `use Spectre.Beam` now adds a
  local endpoint unless disabled with `local: false`. `Spectre.Beam.open/2`,
  `ask/3`, `Spectre.Beam.Chat.open/2`, and the IEx helpers accept an Agent
  module directly and lazily start its gateway under Beam supervision, so
  IEx and LiveView need no manually declared gateway child.
- **Gateway runtime.** `Spectre.Beam.Gateway` is a supervised process plane
  over the existing function boundary: it owns the mounted endpoints, resolves
  the provider client once instead of on every call, serializes each
  conversation, and delivers through a bounded per-endpoint outbox. The
  caller-owned API (`Spectre.Beam.handle/4`, `decode/4`, `deliver/4`) is
  unchanged, and a host that never starts a gateway starts none of these
  processes.
- `Spectre.Beam.Conversation` — one process per `{endpoint, conversation}`.
  Concurrent messages on one chat can no longer run overlapping turns. Adds
  optional coalescing of a burst into a single turn, cancellation of the turn
  in flight, a typing lifecycle bracketing the real turn, and a bounded
  transcript for surfaces that reconnect.
- `Spectre.Beam.Outbox` — per-endpoint delivery queue. Delivery no longer
  blocks its caller for the reply delay, throttle reservation and retry
  budget. The queue is bounded with a declared overflow policy
  (`:reject` or `:drop_oldest`), so a stalled provider produces an observable
  failure instead of unbounded memory growth.
- `Spectre.Beam.Ref`, `Spectre.Beam.Event` and `Spectre.Beam.Bus` — the
  address, the closed event contract, and the fan-out every surface shares.
  Events carry a monotonic per-conversation `seq`, so a reconnecting client
  replays from a cursor instead of guessing.
- `Spectre.Beam.Chat` — the OTP surface for LiveView, IEx, CLI, and tests:
  `open/3`, `send/3` (asynchronous), `ask/3`, `subscribe/1`, `history/2`,
  `cancel/1`, `push/3`, `close/1`.
- `Spectre.Beam.Console` and `Spectre.Beam.IEx` — an interactive terminal
  conversation plus one-line shell helpers (`say`, `ask`, `ls`, `endpoints`,
  `tail`, `focus`, `doctor`), addressing a current conversation kept in the
  shell process.
- `Spectre.Beam.Socket.Server` and `Spectre.Beam.Socket.Client` — a local
  control socket (Unix domain, `0600`, or loopback TCP) speaking length-framed
  JSON, with ETF as the fallback when Jason is absent. Makes the gateway
  drivable from any language without joining the cluster.
- `mix beam.chat`, `mix beam.send`, `mix beam.status`, `mix beam.tail` and
  `mix beam.doctor`, each usable in-VM or against a running release through
  `--socket`.
- `Spectre.Beam.Adapters.Local` and `Spectre.Beam.Adapters.Test` — the in-VM
  channel behind every local surface, and the test channel that lets a suite
  drive a real gateway rather than a parallel code path.
- `Spectre.Beam.Store.ETS` — idempotency store with bounded retention. It adds
  `:ttl_ms` for completed claims and `:claim_ttl_ms` so a claim abandoned by a
  crashed caller heals instead of fencing its key forever. Gateways default to
  a private instance of it.
- `Spectre.Beam.Doctor` — checks the runtime processes, the configured agent
  and store, and every endpoint's adapter, server and outbox.
- `Spectre.Beam.Telemetry` — optional `:telemetry` spans for ingress, turn,
  delivery and conversation lifecycle.
- `Spectre.Beam.Runtime.observable_reply/2`, `turn/3`, `finish_decode/4` and
  `spectre_available?/0`, the public pieces a gateway conversation needs to
  run a turn and deliver its reply asynchronously.

### Changed

- `Spectre.Beam.Application` now supervises a unique registry, a duplicate-key
  bus registry, a task supervisor and the sequence table alongside the
  existing store and pacer.
- `Spectre.Beam.reply/4` is implemented on top of the new
  `Runtime.observable_reply/2`, and a legacy result whose reply text is not a
  binary is treated as having nothing observable to send instead of raising.
- Added `{:jason, "~> 1.4", optional: true}`. It is used only for JSON framing
  on the control socket and is never required.

### Fixed

- Lazy Agent gateways now rehydrate provider runtime settings retained by the
  compiled endpoint, including `client`, `ingress`, coalescing, queue bounds,
  and overflow policy. ExGram and ExWapp therefore keep the same resolved
  client and subscription lifecycle as explicitly supervised gateways.


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
