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

## Who owns the loop

`Spectre.Stack` installs immutable capabilities and tool descriptors. The Agent
selects that Stack and declares routing; `Spectre.Server` owns the GenServer,
conversation, pending continuations and asynchronous results. Use
`use Spectre.Server, tools: MyApp.AI` to generate private helpers. A helper returns
an updated context containing a command; return that context from the callback.
It does not synchronously return the package's result. Tuple input chooses the
operation, and `on_result:` identifies the continuation in `handle_result/3`.

Preparation validates ordinary input before dispatch. The Server records command
identity/outbox state, invokes a worker, projects a bounded portable result and
acquires that result before the callback. Durable recovery needs a configured
checkpoint Store. Keep live clients and process handles in supervised resources
or process-local private context, not durable assigns. Native OTP callbacks remain
available; they use the same command-return boundary.

## Where GAM intervenes

The [Governed Act Model](https://spectre.guide/en/governed-act-model) separates
authority (Mandate), supporting facts (Evidence), the admitted execution record
(Act), and unresolved obligations (Duty). Installing a package connects a
capability; it does not grant authority to act on behalf of the end user.

A cognitive descriptor uses the tool worker path, without treating every model
call or classifier as a consent request. A governed descriptor becomes `propose`
before dispatch and uses the standard `Spectre.Server.GAM` bridge. Host-declared
surfaces still determine which effects, protected disclosures or resource uses
need governance; read-oriented operations are not categorically exempt from those
application boundaries. Ordinary configured cognition is not automatically a new
end-user authorization ceremony.

AgentServer automatically owns the GAM scope, Candidate preparation, authority
checks, consent Presentations, admitted execution and recorded results. Mounted
Skill Servers use that same Domain and authority ledger. The application enables
operations, supplies authenticated identity and business restrictions, and
presents the Server's consent requests; it does not implement a GAM Host.
`bind:` selects an operation reference and grants no authority. Native package
calls outside the Server do not acquire these guarantees automatically. Use the
[core automatic GAM APIs](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/MANAGED_GAM.md)
when an external process needs governed execution; directly invoking an effect
executor in a flow callback bypasses that boundary.

## Recovery and errors

These packages' cognitive helper operations declare safe replay; the runtime can
retry interrupted cognitive work according to the command lifecycle. A result is
still scoped to its original command. Application input rejections arrive as
`{:error, {:tool_rejected, tool, operation, reason}}`; library domain errors retain
the package's tagged result. Invalid installation/configuration is a developer
error, not a successful acquisition. The general `{:tool_unknown, details}` result
belongs to nonrepeatable tool operations; it is not an instruction to retry effects.

Governed effects use GAM recovery and their original command identities. An
ambiguous outcome remains unresolved: it must not be reported as proof of failure
or sent again blindly. A changed checkpoint fingerprint needs an explicit host
migration. Contract drift is handled by the core recovery rules rather than
invoking new adapter code with old arguments. See the
[core lifecycle](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/STACK_TOOLS.md)
and [AgentServer guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/AGENT_SERVER.md).

## Mounted Skill runtimes

Use the Stack-generated `beam` helper in a Skill's ordinary
`Spectre.Server` callbacks. The Agent mounts `Spectre.Skill, server: ...`; each
mount then owns its GenServer, state, workers, streams, pending intentions and
checkpoint key. No package-specific Skill adapter or extra installation is needed.

```text
AgentServer ─────┐
Research Skill ──┼──→ one automatic GAM boundary → same Domain / authority ledger
Knowledge Skill ┘
```

The root creates this connection once and passes it to the children. Do not
bootstrap another Domain, copy the ledger, implement a Skill Host or extend
ingress manually. Mounting grants no authority. Governed effects must satisfy
both root `gam:` operations and the mount's `governed:` references; scoped
Kinetic references keep their complete mounted scope. Ledger/Lab inspect the
calling runtime's checkpoint, not a copy of the shared GAM history.

The shared kernel orders admission while workers can run concurrently. Every
proposal retains its Skill invocation provenance. A confirmation belongs to the
originating mount and exact presentation, so it cannot approve a different
Skill's pending request. Timer expiry and closure enter the same lifecycle
ledger without erasing Acts, uncertain Outcomes or Duties. No per-Skill GAM
configuration is required.

See the [core shared/local ownership table and confirmation example](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/SKILLS.md),
[automatic GAM contract](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/MANAGED_GAM.md)
and [LiveView showcase](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/DEMO.md).
