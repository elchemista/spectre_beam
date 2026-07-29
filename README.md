# Spectre Beam

`spectre_beam` is the external-channel boundary package for Spectre. It
normalizes inbound provider events, preserves endpoint/conversation affinity
for replies, and contributes policy-controlled proactive delivery actions.

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
