# Beam with Spectre 0.4 AgentServer

[README](../README.md) · [Migration](MIGRATING_TO_0_4.md) · [Public API](PUBLIC_API.md)

## Helper contract

| Input | Path | Callback result |
| --- | --- | --- |
| `{:endpoints}` | Cognitive | `{:ok, [%{id: id, type: type, capabilities: list}]}` |
| `{:deliver, endpoint, attrs}` | Governed when bound | GAM result, for example `{:governed, %{outcome: %{status: :succeeded}}}` |

There is no default operation. Endpoint discovery calls each installed adapter's
real capability callback. For text deliveries the capability is `:text`, matching
`content.type`; it is not `:send_text`. Both operations have a 1 MiB descriptor
budget. Delivery attributes are restricted to `:to`, `:content`, `:conversation_id`
and `:reply_to`; input cannot replace the adapter or runtime transport configuration.
Unknown, duplicate, nil or non-atom `bind:` entries fail installation.

```elixir
install Spectre.Beam, bind: [deliver: :send_notice] do
  channel :email, adapter: MyApp.EmailChannel
end
```

`MyApp.EmailChannel` implements the real `Spectre.Beam.Channel` behaviour; it is
application code, not a package-provided email service. Configure the allowed
operation before starting a Server that selects this Stack:

```elixir
{:ok, server} = Spectre.Server.start_link(
  agent: MyApp.SupportAgent,
  identity: %{subject: authenticated_user.id},
  consumer: self(),
  gam: [operations: %{send_notice: [confirm: true]}])
```

AgentServer creates its GAM boundary and frozen presentation automatically.
Application `handle_action/2` may add business restrictions. The registered Beam
adapter receives only an admitted Act and its committed target/content.
Beam pins the idempotency key, makes one provider attempt,
and projects the receipt to ordinary data. Reusing the command id returns the
existing receipt or a fenced in-progress result; it does not manufacture a second
send. `{:error, {:ambiguous, reason}}` must map to a GAM ambiguous outcome with
appropriate evidence, not a claim that no message was delivered.

```elixir
def handle_result(:notice, {:governed, %{outcome: %{status: :succeeded}}}, ctx),
  do: respond(ctx, "The transport accepted the notice.")

def handle_result(:notice, {:governed, %{outcome: %{status: :ambiguous}}}, ctx),
  do: respond(ctx, "Delivery still needs verification.")

def handle_result(tag, result, ctx), do: super(tag, result, ctx)
```

A provider's acceptance is technical delivery evidence, not proof that a recipient
read or acted on the message. Optional consent can use the Server pending callbacks;
its confirmation/resumption belongs to GAM, not to a second Beam policy DSL.

## Ingress and lifecycle

Decode and authenticate the provider event through your channel/pipelines, resolve
the correct host-owned Server for that conversation, then enqueue the normalized
inbound:

```elixir
{:ok, interaction} = Spectre.Beam.enqueue(server, inbound)
```

The Server consumer receives eventual responses. A host may present those locally;
if delivery to an external channel is part of the governed surface, stage a Beam
delivery command through its admitted path. `enqueue/3` neither auto-creates a
session nor auto-delivers a reply. Beam applications supervise their transport and
idempotency resources; credentials stay in host runtime configuration.

Native channel APIs and the retained Gateway/Chat/Instance conveniences are in
[their own guide](CHANNELS_AND_LEGACY_RUNTIME.md). In particular, `Beam.ask` is not
an alias for `Spectre.Server.input`, and its legacy Turn machinery is not silently
replaced by installing the 0.4 helper.

## Server lifecycle and Skills

Return the context produced by the helper. The Server queues the command and
delivers the result to `handle_result/3` with the `on_result:` tag. It owns
checkpointing, pending consent and recovery; package installation grants no
authority. An uncertain effect needs reconciliation rather than a blind retry.

The helper also works in mounted Skill Servers, which share the root GAM Domain
and keep separate state and checkpoints. See [Stack tools](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/STACK_TOOLS.md),
[Managed GAM](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/MANAGED_GAM.md) and the [Skill guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/SKILLS.md)
for the shared runtime contracts. Native APIs remain caller-owned.
