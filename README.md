# Spectre Beam

`spectre_beam` is the external-channel boundary for Spectre applications. It
normalizes provider events, keeps endpoint and conversation affinity, runs
portable boundary pipelines, and delivers reactive or proactive messages with
idempotency.

Beam is made for Spectre, but it deliberately has **no runtime Mix dependency
on `:spectre`**. Its Stack manifest, Agent extension, action provider, identity
bridge, and Turn handler use Spectre's public contracts only when both
libraries are present. The Beam repository includes the Spectre `0.2.0` tag
from GitHub only in `MIX_ENV=test`, so the complete integration remains
covered.

Beam `0.2.0` and the Stack manifest's `spectre: "~> 0.2.0"` field are aligned
with the Spectre `0.2.0` public API. Spectre projects are distributed from
GitHub tags and are not published to Hex.

The supported surface is listed in the
[public API manifest](docs/PUBLIC_API.md).

## Installation

Both projects are consumed directly from their GitHub `0.2.0` tags:

```elixir
def deps do
  [
    {:spectre, github: "elchemista/spectre", tag: "0.2.0"},
    {:spectre_beam, github: "elchemista/spectre_beam", tag: "v0.2.0"}
  ]
end
```

Beam itself declares Spectre only for its integration suite:

```elixir
{:spectre, github: "elchemista/spectre", tag: "0.2.0", only: :test}
```

## Spectre Stack integration

Beam remains directly installable in a Spectre Stack:

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

## Delivery logistics

Conversational channel plumbing — pacing, typing, a human reply delay, and
bounded retries — is configured per channel (Stack-level `install` options set
defaults; every option also accepts a per-call override) and runs while the
outbound idempotency claim is held:

```elixir
channel :whatsapp,
  type: :whatsapp,
  adapter: Spectre.Beam.Adapters.ExWapp,
  typing: true,
  reply_delay_ms: {1_500, 3_000},
  throttle: [
    messages_per_second: 2.0,
    burst: 4,
    min_delay_ms: 400,
    per_conversation: [messages_per_minute: 12],
    jitter_ms: 250
  ],
  retry: [max_attempts: 3, base_delay_ms: 250, max_delay_ms: 5_000]
```

- `typing:` calls the adapter's optional `typing/3` callback right before the
  delay and send, so the pause reads as composing. Best effort: provider
  failures never fail the delivery. The bundled adapters dispatch to the
  provider's `send_typing(client, to, composing?)`.
- `reply_delay_ms:` pauses between the typing signal and the provider call;
  `{min, max}` randomizes the pause.
- `throttle:` reserves a send slot through `Spectre.Beam.Throttle` before the
  adapter is invoked. The bundled `Spectre.Beam.Throttle.Local` paces per
  endpoint (token bucket and `min_delay_ms` spacing) and per conversation;
  `{MyApp.ClusterPacer, config}` swaps the strategy. A rejected reservation
  returns `{:error, {:beam_throttled, endpoint, reason}}` and releases the
  idempotency claim, so the send stays retryable.
- `retry:` re-invokes the adapter with exponential backoff for plain
  `{:error, reason}` replies. `{:error, {:ambiguous, _}}` outcomes are never
  retried — the provider may already have accepted the message — and
  `retry_on: (reason -> boolean)` narrows what is retryable.

Selecting that Stack activates the Beam extension, source constraints,
reactive delivery, and proactive action providers:

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

Provider clients remain caller-owned runtime values:

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

The high-level flow is:

1. Beam decodes ExGram or ExWapp data into `Spectre.Beam.Inbound`.
2. Beam maps that value to Spectre's public input and calls the canonical
   `Spectre.turn/3` boundary.
3. Only an observable reply is converted to `Spectre.Beam.Outbound` and sent
   through the original endpoint.
4. Policy, pending effects, invocations, state, and persistence remain owned
   by Spectre.

## Direct channel boundary

Provider normalization and delivery can also be used without starting an
Agent. This is useful for webhooks, workers, tests, or applications that want
to compose the Spectre call explicitly:

```elixir
beam =
  Spectre.Beam.new(
    telegram: [type: :telegram, adapter: Spectre.Beam.Adapters.ExGram],
    whatsapp: [type: :whatsapp, adapter: Spectre.Beam.Adapters.ExWapp]
  )

{:ok, inbound} =
  Spectre.Beam.decode(beam, :telegram, raw_update,
    adapter_opts: [authenticated?: verified?]
  )

outbound =
  Spectre.Beam.Outbound.new(
    endpoint: :telegram,
    conversation_id: inbound.conversation_id,
    to: inbound.sender,
    reply_to: inbound.message_id,
    content: Spectre.Beam.Content.text("Received"),
    idempotency_key: "telegram-reply-42"
  )

{:ok, receipt} =
  Spectre.Beam.deliver(beam, :telegram, outbound,
    adapter_opts: [client: telegram_session]
  )
```

This mode needs no Spectre module. When the application does use Spectre, it
can place `Spectre.turn/3` between `decode/4` and `deliver/4`, or use
`handle/4` for the integrated path.

## Subject-scoped Agent Instances

The identity-safe path keeps Spectre as the only authority that links an
authenticated provider principal to a Subject. Beam never derives identity
from display names, phone similarity, conversation ids, message text, or model
output:

```elixir
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

The built-in adapters default `authenticated?` to `false`; only the host may
mark a verified event as authenticated.

## Channel adapters

Every provider implements `Spectre.Beam.Channel`:

- `decode/2` normalizes an event into `Spectre.Beam.Inbound`;
- `deliver/2` returns a provider-neutral `Spectre.Beam.Receipt`;
- optional capability, acknowledgement, receipt-normalization, subscribe, and
  unsubscribe callbacks describe provider lifecycle.

`Spectre.Beam.Adapters.ExGram` and `Spectre.Beam.Adapters.ExWapp` invoke their
provider modules dynamically, so Beam does not force either SDK into the
dependency graph. Pass `module:` for a compatible wrapper and `client:` or
`session:` at runtime. An ExWapp acknowledgement timeout is reported as
ambiguous, preventing an unsafe automatic retry.

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

A plug may replace the current value, add pipeline-local assigns, or halt. It
cannot change the stage or endpoint identity.

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
