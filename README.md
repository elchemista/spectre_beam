# Spectre Beam

Beam is the external-channel boundary: it decodes provider events, preserves
conversation and endpoint identity, runs boundary pipelines, and delivers messages
with idempotency receipts. It can also be used directly as an Elixir channel layer;
Beam deliberately has no runtime Mix dependency on Spectre.

With Spectre 0.4, the Stack installs channel capabilities, the Agent declares
routes, and a separate AgentServer owns conversation callbacks. Beam supplies
transport operations; the Server and GAM own consequential agent actions.

## Installation on the 0.4 development branch

This branch targets the **unreleased Spectre 0.4 architecture**. Use matching Git
branches; the examples do not assume that Spectre 0.4 is available from Hex.
For reproducible deployments, replace branch selectors with reviewed commit refs.

```elixir
def deps do
  [
    {:spectre, github: "elchemista/spectre",
     branch: "feature/agent-server-gam", override: true},
    {:spectre_beam, github: "elchemista/spectre_beam",
     branch: "feature/stack-tools-0.4"}
  ]
end
```

The manifest requirement `{:spectre, "~> 0.4.0"}` describes compatibility, not
publication availability. Local contributors can use `SPECTRE_PATH=../spectre` for this repository's dependency override.

## A channel inside AgentServer

This example discovers a local channel without requiring credentials or a model:

```elixir
defmodule MyApp.AI do
  use Spectre.Stack
  install Spectre.Beam do
    channel :console, adapter: Spectre.Beam.Adapters.Local
  end
end

defmodule MyApp.SupportServer do
  use Spectre.Server, tools: MyApp.AI

  def handle_flow({:support, :channels}, _input, ctx),
    do: beam(ctx, {:endpoints}, on_result: :channels)

  def handle_result(:channels, {:ok, endpoints}, ctx),
    do: respond(ctx, inspect(endpoints))

  def handle_result(tag, result, ctx), do: super(tag, result, ctx)
end

defmodule MyApp.SupportAgent do
  use Spectre.Agent, stack: MyApp.AI, server: MyApp.SupportServer
  flow :support do
    on :channels, regex: ~r/^channels$/i
  end
end

{:ok, server} = Spectre.Server.start_link(agent: MyApp.SupportAgent, consumer: self())
{:ok, interaction} = Spectre.Server.input(server, "channels")
```

The core generates `beam/3` from the Stack registration; there is no
`import Spectre.Beam, only: [beam: 3]`. The callback receives a portable list of
`%{id: ..., type: ..., capabilities: [...]}`. The response reaches the consumer as
`{Spectre.Server, server, {:response, interaction, text, effects}}`.

## Receiving and sending

After the provider adapter and host have decoded and authenticated an event,
`Spectre.Beam.enqueue(server, inbound)` queues a `%Spectre.Beam.Inbound{}` on the
existing Server and returns `{:ok, interaction}`. It does not create a legacy Turn,
select the correct user session for you, or automatically send the Server's
response to a provider. The host owns those connections.

To expose delivery, change the installation to
`install Spectre.Beam, bind: [deliver: :send_notice] do ... end`. In a Server
callback the command is:

```elixir
beam(ctx,
  {:deliver, :console,
   %{to: "local", content: %{type: :text, text: "Your report is ready."}}},
  on_result: :notice)
```

This stages a governed proposal. Start that Server with
`gam: [operations: %{send_notice: [confirm: true]}]`; installation alone does not
authorize a message. Spectre owns its GAM integration and registered execution. See the [complete boundary and result contract](docs/SPECTRE_0_4.md).
Direct native `Spectre.Beam.deliver/4` remains available outside this loop, where
the caller owns authorization, execution and recovery.

## Documentation

- [AgentServer integration, helpers and authority](docs/SPECTRE_0_4.md)
- [Migration from the 0.3 integration](docs/MIGRATING_TO_0_4.md)
- [Standalone and retained runtime reference](docs/CHANNELS_AND_LEGACY_RUNTIME.md)
- [Public API](docs/PUBLIC_API.md) and [changelog](CHANGELOG.md)
- [Governed Act Model](https://spectre.guide/en/governed-act-model) and [core tool lifecycle](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/STACK_TOOLS.md)

## Use inside a reusable Skill

The `beam` helper is generated in a Skill callback module with
`use Spectre.Server, tools: MyApp.AI`, using the same installed Stack. A
`Spectre.Skill` with `server:` has a separate GenServer, local assigns, workers,
streams, pending intentions and checkpoint key.

**It uses the root AgentServer's same automatic GAM integration, Domain and
authority ledger.** Configure `gam:` once on the root. Mounting a Skill creates
no new Domain or grant and needs no additional Host or consent ingress. Shared
Stack resources stay in their existing supervised runtime.

Agent and Skill workers can run concurrently; the shared kernel orders admission.
A governed effect must be enabled in root `operations` and permitted by the
mount's `governed:` references. Each Skill proposal records its mount and
invocation provenance. Consent identifies the originating Skill and exact
presentation: approving an email cannot approve another Skill's memory write.
Timer closure is recorded in the same Domain and does not erase a Duty.
Ordinary cognitive operations retain their configured provider boundary.

See the [core Skill ownership and consent guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/SKILLS.md),
[automatic GAM responsibilities](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/MANAGED_GAM.md)
and [two-Skill LiveView showcase](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/DEMO.md).

## Managed GAM execution registration

The optional `c:Spectre.Stack.Installable.effects/1` callback registers immutable
execution adapters for Spectre 0.4 managed GAM. Beam registers only a `deliver` operation selected by `bind:`. The admitted Act supplies a stable correlation/idempotency key; transport uncertainty remains explicit.

The effect registration is included in the compiled installation digest and is
checked again before execution. It grants no authority: the application enables
operations and the GAM kernel admits each concrete consequence before the adapter
runs. AgentServer owns GAM automatically in every environment. Native package
APIs remain available outside the AgentServer loop; their caller owns that boundary.
Adapters implement `execute_effect/3` and distinguish successful observations,
definitive no-effect and uncertain outcomes; uncertain effects are not retried.
