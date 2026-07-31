# Spectre Beam public API — 0.1.6 baseline

This file is the normative public API manifest for the recoverable `0.1.6`
baseline. Compatibility guarantees apply only to the modules and callables
listed below. Any module, function, macro, or callback not listed here is an
implementation detail even when it is exported or visible in generated docs.

Default arguments are expanded into every callable arity. For the listed
modules, documented types, opaque types, and documented struct fields are also
public. Modules with no callable row expose only their documented module,
type, and struct contract.

## Manifest

- `Spectre.Beam`
  - functions: `config/1`, `decode/3`, `decode/4`, `external_identity/1`, `external_identity/2`, `handle/3`, `handle/4`, `handle_instance/4`, `handle_instance/5`, `reply/3`, `reply/4`, `resolve_instance/3`, `resolve_instance/4`, `subscribe/2`, `subscribe/3`, `to_input/1`, `unsubscribe/2`, `unsubscribe/3`, `version/0`
  - macros: `beam/2`, `beaming/1`, `channel/2`
- `Spectre.Beam.ActionProvider`
- `Spectre.Beam.Adapters.ExGram`
- `Spectre.Beam.Adapters.ExWapp`
- `Spectre.Beam.Channel`
  - callbacks: `acknowledge/2`, `capabilities/1`, `decode/2`, `deliver/2`, `normalize_receipt/2`, `subscribe/1`, `unsubscribe/1`
- `Spectre.Beam.Config`
- `Spectre.Beam.Content`
- `Spectre.Beam.Endpoint`
  - functions: `capabilities/1`, `pipeline/2`
- `Spectre.Beam.Exchange`
- `Spectre.Beam.IdempotencyStore`
  - callbacks: `claim/2`, `complete/3`, `release/2`
- `Spectre.Beam.Identity`
  - functions: `external_identity/1`, `external_identity/2`, `resolve_instance/3`, `resolve_instance/4`
- `Spectre.Beam.Inbound`
- `Spectre.Beam.Outbound`
- `Spectre.Beam.Pipeline`
  - functions: `assign/3`, `halt/1`, `halt/2`, `put_value/2`, `run/4`, `run/5`, `validate_specs!/2`
- `Spectre.Beam.Plug`
  - callbacks: `call/2`, `init/1`
- `Spectre.Beam.Receipt`
- `Spectre.Beam.Runtime`
  - functions: `handle_instance/4`, `handle_instance/5`, `subscribe/2`, `subscribe/3`, `unsubscribe/2`, `unsubscribe/3`
- `Spectre.Beam.Store`
  - functions: `child_spec/1`
- `Spectre.Beam.TargetResolver`
  - callbacks: `resolve/3`
