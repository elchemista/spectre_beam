# Spectre Beam

`spectre_beam` is the external-channel boundary package for Spectre. It
normalizes inbound provider events, preserves endpoint/conversation affinity
for replies, and contributes policy-controlled proactive delivery actions.

The exact `0.2.0` compatibility surface is published in the
[public API manifest](docs/PUBLIC_API.md).

## 0.2.0 Spectre Compatibility

Version `0.2.0` aligns Beam's package and Stack contracts with Spectre
`~> 0.2.0`. The channel, identity, reactive reply, and proactive Effect
boundaries remain unchanged; the permanent `0.1.6` exchange fixture continues
to verify their recovery shape against the new core runtime.

## 0.1.6 Recoverable Baseline

Version `0.1.6` is a consolidation-only release with no new runtime feature and
no intentional breaking change. Elixir 1.19 on Erlang/OTP 28 is the initially
guaranteed pair. Uniform CI runs format, warnings-as-errors compilation, tests,
non-strict Credo, Dialyzer, ExDoc, and local package validation with no
publication. The permanent Beam
exchange fixture under `test/fixtures/compatibility/0.1.6` freezes the handoff
shape used by the future `0.2.0` work.

## Installation

The project is distributed from GitHub:

```elixir
def deps do
  [
    {:spectre_beam, github: "elchemista/spectre_beam"}
  ]
end
```

## Stack

```elixir
defmodule MyApp.AI do
  use Spectre.Stack

  install Spectre.Beam do
    channel :telegram,
      type: :telegram,
      adapter: Spectre.Beam.Adapters.ExGram,
      before_decode: [MyApp.VerifyTelegramSignature],
      inbound_pipeline: [MyApp.EnrichSender]

    channel :whatsapp,
      type: :whatsapp,
      adapter: Spectre.Beam.Adapters.ExWapp,
      outbound_pipeline: [MyApp.RedactOutbound]
  end
end
```

Selecting the Stack activates the complete Beam Agent adapter: endpoint
configuration, source-specific Flow constraints, reactive delivery, proactive
Action providers, and the Beam handler syntax. A second `use Spectre.Beam` is
not required:

```elixir
defmodule MyApp.Agent do
  use Spectre.Agent, stack: MyApp.AI

  flow :telegram_support, beam: :telegram do
    on :question, regex: ~r/question/i do
      reply "How can I help?"
    end
  end

  flow :notify do
    on :notify, regex: ~r/^notify$/ do
      beam :owner, via: :whatsapp, text: "The report is ready"
    end
  end
end
```

Provider clients and subscriptions remain caller-owned runtime values:

```elixir
{:ok, exchange} =
  Spectre.Beam.handle(MyApp.Agent, :telegram, raw_update,
    adapter_opts: [client: telegram_session]
  )

:ok =
  Spectre.Beam.subscribe(MyApp.Agent, :whatsapp,
    adapter_opts: [client: whatsapp_session]
  )
```

## Subject-scoped Agent Instances

Version 0.2.0 keeps the explicit identity-safe path for multichannel
continuity and routes each channel conversation into the matching core Run.
Beam authenticates and normalizes the provider principal into a
`Spectre.ExternalIdentity`; the core Subject Registry must already contain an
explicit link before the inbound can reach an Agent Instance:

```elixir
verified? = MyApp.TelegramAuth.verify(raw_update)

{:ok, inbound} =
  Spectre.Beam.decode(MyApp.Agent, :telegram, raw_update,
    adapter_opts: [authenticated?: verified?]
  )
{:ok, external_identity} =
  Spectre.Beam.external_identity(inbound,
    authenticated_at: System.system_time(:millisecond),
    proof_ref: provider_signature_id
  )

{:ok, _link} =
  Spectre.Subject.Registry.bind(
    Spectre.Subject.Registry,
    MyApp.Agent,
    Spectre.Subject.new(account.id),
    external_identity,
    proof: verified_enrollment_id
  )

{:ok, exchange} =
  Spectre.Beam.handle_instance(
    MyApp.SpectreSupervisor,
    MyApp.Agent,
    :telegram,
    raw_update,
    adapter_opts: [client: telegram_session, authenticated?: verified?]
  )
```

`handle_instance/5` fails closed for an unauthenticated or unlinked identity.
The built-in adapters default `authenticated?` to `false`; the host may set it
to `true` only after its provider authentication step succeeds.
It never derives a Subject from a conversation id, sender similarity, display
name, address book, message text, or model decision. Linking another channel
uses the core `LinkIntent` challenge flow; Beam only transports that proof.
The existing `handle/4` API remains available for stateless and legacy
caller-owned Session integrations.

Beam delivers only an observable
`{:reply, output, %Spectre.Run.Ref{}}` boundary. Policy and invocation
boundaries (`{:needs, boundary}` and `{:awaiting, invocation_ref}`) are
returned to the host without leaking any intermediate `result.reply_text`.
The outbound idempotency key is derived from
`Spectre.Run.Ref.token/1`, so retrying the same boundary cannot deliver it
twice.

Beam never automatically executes a staged proactive effect. It remains
subject to Spectre policy, durability, and idempotency:

```elixir
{:ok, result} = Spectre.ask(MyApp.Agent, "notify")
{:ok, completed} =
  Spectre.execute(MyApp.Agent, result,
    adapter_opts: [client: whatsapp_session]
  )
```

Installing Beam activates the implementation; it does not grant an external
identity, authorize an endpoint, expose every operation to a planner, or place
provider credentials in the compiled Stack.

## Channel adapters

Every provider implements the small `Spectre.Beam.Channel` contract:

- `decode/2` normalizes an event into `Spectre.Beam.Inbound`;
- `deliver/2` returns a provider-neutral `Spectre.Beam.Receipt`;
- optional capability, acknowledgement, receipt-normalization, subscribe, and
  unsubscribe callbacks describe provider lifecycle without changing Beam.

`Spectre.Beam.Adapters.ExGram` and `Spectre.Beam.Adapters.ExWapp` are included
and call compatible provider modules dynamically, so Beam does not force
either SDK into the dependency graph. Pass `module:` when wrapping a compatible
API and pass `client:` or `session:` at runtime. An ExWapp acknowledgement
timeout is reported as ambiguous, preventing an unsafe automatic retry.

Custom providers implement the same behavior:

```elixir
defmodule MyApp.Channel do
  @behaviour Spectre.Beam.Channel

  def decode(event, opts), do: MyApp.Normalize.inbound(event, opts)
  def deliver(outbound, opts), do: MyApp.Provider.send(outbound, opts)
end
```

## Plug-style pipelines

Endpoints accept ordered plugs at four stable stages:

- `:before_decode` for raw provider events;
- `:after_decode` (`inbound_pipeline`) for normalized inbound values;
- `:before_deliver` (`outbound_pipeline`) for outbound values;
- `:after_deliver` (`receipt_pipeline`) for receipts.

A plug implements `Spectre.Beam.Plug` and returns the pipeline after replacing
its value, adding assigns, or halting. Pipeline identity is immutable: a plug
cannot switch stage or endpoint. Authentication, filtering, enrichment,
redaction, metrics, and provider-specific compatibility belong here; policy,
Agent state, retry decisions, and delivery remain owned by their existing
boundaries.

```elixir
defmodule MyApp.EnrichSender do
  @behaviour Spectre.Beam.Plug
  alias Spectre.Beam.Pipeline

  def call(%Pipeline{stage: :after_decode, value: inbound} = pipeline, _opts) do
    inbound = %{inbound | metadata: Map.put(inbound.metadata, :tenant, :acme)}
    Pipeline.put_value(pipeline, inbound)
  end

  def call(pipeline, _opts), do: pipeline
end
```
