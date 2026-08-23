# Spectre Beam public API — 0.1.0

This manifest describes the supported `spectre_beam` `0.1.0` surface. Beam has
no runtime Mix dependency on Spectre. Its Spectre-facing callbacks are
nevertheless public and are tested against Spectre Hex `~> 0.3.3` through a
test-only dependency.

Default arguments expand into every callable arity. Documented types and
documented struct fields on the listed modules are public. Anything not listed
is an implementation detail even when exported.

## Manifest

- `Spectre.Beam`
  - functions: `ask/2`, `ask/3`, `compile/3`, `config/1`, `decode/3`, `decode/4`, `deliver/3`, `deliver/4`, `external_identity/1`, `external_identity/2`, `handle/3`, `handle/4`, `handle_instance/4`, `handle_instance/5`, `manifest/0`, `new/1`, `new/2`, `open/1`, `open/2`, `reply/3`, `reply/4`, `resolve_instance/3`, `resolve_instance/4`, `subscribe/2`, `subscribe/3`, `to_input/1`, `unsubscribe/2`, `unsubscribe/3`, `version/0`
  - macros: `beam/2`, `beaming/1`, `channel/2`
- `Spectre.Beam.ActionProvider`
  - Spectre callbacks: `actions/1`, `execute/3`, `schema_hash/2`
- `Spectre.Beam.Adapters.ExGram`
- `Spectre.Beam.Adapters.ExWapp`
- `Spectre.Beam.Channel`
  - callbacks: `acknowledge/2`, `capabilities/1`, `decode/2`, `deliver/2`, `normalize_receipt/2`, `subscribe/1`, `typing/3`, `unsubscribe/1`
- `Spectre.Beam.Config`
  - functions: `fetch/2`, `new/1`, `new/2`
- `Spectre.Beam.Content`
  - functions: `modalities/1`, `new/1`, `text/1`, `text/2`
- `Spectre.Beam.Endpoint`
  - functions: `adapter_opts/1`, `adapter_opts/2`, `capabilities/1`, `new/2`, `pipeline/2`
- `Spectre.Beam.Exchange`
- `Spectre.Beam.Extension`
  - Spectre extension callbacks: `action_providers/1`, `agent_config/1`, `api_version/0`, `compile/2`, `expand_handler/3`, `flow_constraints/2`, `id/0`, `setup/2`
- `Spectre.Beam.IdempotencyStore`
  - callbacks: `claim/2`, `complete/3`, `release/2`
- `Spectre.Beam.Identity`
  - functions: `external_identity/1`, `external_identity/2`, `resolve_instance/3`, `resolve_instance/4`
- `Spectre.Beam.Inbound`
  - functions: `conversation_key/1`, `key/1`, `new/1`
- `Spectre.Beam.Outbound`
  - functions: `new/1`
- `Spectre.Beam.Pipeline`
  - functions: `assign/3`, `halt/1`, `halt/2`, `put_value/2`, `run/4`, `run/5`, `validate_specs!/2`
- `Spectre.Beam.Plug`
  - callbacks: `call/2`, `init/1`
- `Spectre.Beam.Receipt`
  - functions: `accepted/1`, `accepted/2`, `new/1`
- `Spectre.Beam.Runtime`
  - functions: `decode/3`, `decode/4`, `deliver/3`, `deliver/4`, `finish_decode/3`, `finish_decode/4`, `handle/3`, `handle/4`, `handle_instance/4`, `handle_instance/5`, `observable_reply/2`, `reply/3`, `reply/4`, `spectre_available?/0`, `subscribe/2`, `subscribe/3`, `to_input/1`, `turn/2`, `turn/3`, `unsubscribe/2`, `unsubscribe/3`
- `Spectre.Beam.Store`
  - functions: `child_spec/1`
- `Spectre.Beam.TargetResolver`
  - callbacks: `resolve/3`
- `Spectre.Beam.Throttle`
  - callbacks: `reserve/3`
- `Spectre.Beam.Throttle.Local`
  - functions: `child_spec/1`, `reset/0`, `reset/1`

## Gateway runtime

The modules below make Beam a supervised gateway. They are additive: the
function boundary above keeps working unchanged, and a host that never starts
`Spectre.Beam.Gateway` never starts any of these processes.

- `Spectre.Beam.Gateway`
  - functions: `close/2`, `conversations/1`, `decode/3`, `decode/4`, `deliver/3`, `deliver/4`, `endpoints/1`, `ensure/1`, `ensure/2`, `ingest/3`, `ingest/4`, `ingest_inbound/2`, `ingest_inbound/3`, `list/0`, `open/2`, `open/3`, `push/3`, `push/4`, `resolve/3`, `spec/1`, `start_link/1`, `stop/1`
- `Spectre.Beam.Gateway.Spec`
  - functions: `agent_for/2`, `channel/2`, `endpoint/2`, `endpoints/1`, `new/1`
- `Spectre.Beam.Chat`
  - functions: `ask/2`, `ask/3`, `cancel/1`, `close/1`, `history/1`, `history/2`, `open/1`, `open/2`, `open/3`, `push/2`, `push/3`, `send/2`, `send/3`, `status/1`, `subscribe/1`, `subscribe_endpoint/2`, `unsubscribe/1`
- `Spectre.Beam.Conversation`
  - functions: `cancel/1`, `history/1`, `history/2`, `ingest/2`, `ingest/3`, `name/1`, `publish/3`, `status/1`, `whereis/1`
- `Spectre.Beam.Outbox`
  - functions: `drain/2`, `drain/3`, `enqueue/3`, `enqueue/4`, `info/2`, `name/2`
- `Spectre.Beam.Endpoint.Server`
  - functions: `adapter_opts/2`, `name/2`, `record_event/2`, `status/2`
- `Spectre.Beam.Ref`
  - functions: `endpoint_topic/1`, `from_inbound/1`, `from_inbound/2`, `key/1`, `new/1`, `parse/1`, `parse/2`, `parse!/1`, `parse!/2`, `slug/1`, `topic/1`
- `Spectre.Beam.Event`
  - functions: `new/2`, `new/3`, `new/4`, `to_map/1`, `types/0`, `version/0`
- `Spectre.Beam.Bus`
  - callbacks: `broadcast/3`, `subscribe/2`, `unsubscribe/2`
  - functions: `default/0`, `normalize/1`, `publish/2`, `publish/3`, `subscribe/2`, `unsubscribe/2`
- `Spectre.Beam.Bus.Local`
- `Spectre.Beam.Sequence`
  - functions: `current/1`, `forget/1`, `next/1`
- `Spectre.Beam.Store.ETS`
  - functions: `child_spec/1`, `reset/0`, `reset/1`, `size/0`, `size/1`, `sweep/0`, `sweep/1`
  - callbacks: `claim/2`, `complete/3`, `release/2`
- `Spectre.Beam.Telemetry`
  - functions: `emit/3`, `span/3`
- `Spectre.Beam.Doctor`
  - functions: `report/0`, `report/1`, `run/0`, `run/1`, `verdict/1`

## Local surfaces

- `Spectre.Beam.Adapters.Local`
  - functions: `attach/1`, `attach/2`, `attached/1`, `attached/2`, `detach/1`, `detach/2`
- `Spectre.Beam.Adapters.Test`
  - functions: `attach/1`, `attach/2`, `detach/1`, `detach/2`, `next_delivery/0`, `next_delivery/1`, `next_text/0`, `next_text/1`, `send_inbound/3`, `send_inbound/4`
- `Spectre.Beam.Console`
  - functions: `chat/0`, `chat/1`, `chat/2`, `open/1`, `open/2`, `tail/1`, `tail/2`
- `Spectre.Beam.IEx`
  - functions: `ask/1`, `ask/2`, `ask/3`, `cancel/0`, `cancel/1`, `chat/0`, `chat/1`, `chat/2`, `current/0`, `doctor/0`, `doctor/1`, `endpoints/0`, `endpoints/1`, `focus/1`, `gateways/0`, `history/0`, `history/1`, `history/2`, `ls/0`, `ls/1`, `push/2`, `push/3`, `say/1`, `say/2`, `say/3`, `status/0`, `status/1`, `tail/0`, `tail/1`, `tail/2`
- `Spectre.Beam.CLI`
  - functions: `ask/4`, `chat/3`, `connect/1`, `disconnect/1`, `doctor/1`, `parse/1`, `push/4`, `status/1`, `switches/0`, `tail/3`
- `Spectre.Beam.Socket.Server`
  - functions: `address/1`, `name/1`
- `Spectre.Beam.Socket.Client`
  - functions: `close/1`, `connect/1`, `connect/2`, `follow/3`, `follow/4`, `request/2`, `request/3`, `request/4`
- `Spectre.Beam.Socket.Protocol`
  - functions: `event_frame/1`, `handle/2`, `init/1`, `subscribed?/2`
- `Spectre.Beam.Socket.Codec`
  - callbacks: `decode/1`, `encode/1`
  - functions: `decode/2`, `default/0`, `encode/2`, `json_available?/0`

Mix tasks `beam.chat`, `beam.send`, `beam.status`, `beam.tail` and `beam.doctor`
are part of the supported surface; their options are documented on each task.

## Delivery logistics options

`channel/2` (and `install Spectre.Beam` defaults, and per-call runtime opts)
accept `typing:`, `reply_delay_ms:`, `retry:`, and `throttle:`. Their semantics
are documented on `Spectre.Beam.Logistics` and `Spectre.Beam.Throttle`.

## Optional dependencies

Beam still declares no runtime dependency of its own beyond `:jason`, which is
`optional: true`. Jason enables JSON framing on the local control socket; when
it is absent the socket falls back to ETF, which only Elixir clients read.
`:telemetry` is used when the host installs it and is never required.

## Compatibility boundary

The `spectre: "~> 0.3.3"` value returned by `Spectre.Beam.manifest/0` is Stack
compatibility metadata. It does not create a Mix dependency. Applications that
use the integrated Agent path must include Spectre from Hex and Beam from its
GitHub release; callers using only `new/2`, `decode/4`, and `deliver/4` do not
need Spectre.
