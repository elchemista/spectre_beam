# Spectre Beam public API — 0.2.0

This manifest describes the supported `spectre_beam` `0.2.0` surface. Beam has
no runtime Mix dependency on Spectre. Its Spectre-facing callbacks are
nevertheless public and are tested against the `elchemista/spectre` GitHub
`0.2.0` tag through a test-only dependency.

Default arguments expand into every callable arity. Documented types and
documented struct fields on the listed modules are public. Anything not listed
is an implementation detail even when exported.

## Manifest

- `Spectre.Beam`
  - functions: `compile/3`, `config/1`, `decode/3`, `decode/4`, `deliver/3`, `deliver/4`, `external_identity/1`, `external_identity/2`, `handle/3`, `handle/4`, `handle_instance/4`, `handle_instance/5`, `manifest/0`, `new/1`, `new/2`, `reply/3`, `reply/4`, `resolve_instance/3`, `resolve_instance/4`, `subscribe/2`, `subscribe/3`, `to_input/1`, `unsubscribe/2`, `unsubscribe/3`, `version/0`
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
  - functions: `decode/3`, `decode/4`, `deliver/3`, `deliver/4`, `handle/3`, `handle/4`, `handle_instance/4`, `handle_instance/5`, `reply/3`, `reply/4`, `subscribe/2`, `subscribe/3`, `to_input/1`, `unsubscribe/2`, `unsubscribe/3`
- `Spectre.Beam.Store`
  - functions: `child_spec/1`
- `Spectre.Beam.TargetResolver`
  - callbacks: `resolve/3`
- `Spectre.Beam.Throttle`
  - callbacks: `reserve/3`
- `Spectre.Beam.Throttle.Local`
  - functions: `child_spec/1`, `reset/0`, `reset/1`

## Delivery logistics options

`channel/2` (and `install Spectre.Beam` defaults, and per-call runtime opts)
accept `typing:`, `reply_delay_ms:`, `retry:`, and `throttle:`. Their semantics
are documented on `Spectre.Beam.Logistics` and `Spectre.Beam.Throttle`.

## Compatibility boundary

The `spectre: "~> 0.2.0"` value returned by `Spectre.Beam.manifest/0` is Stack
compatibility metadata. It does not create a Mix dependency. Applications that
use the integrated Agent path must include both GitHub dependencies; callers
using only `new/2`, `decode/4`, and `deliver/4` do not need Spectre.
