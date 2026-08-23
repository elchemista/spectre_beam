# Spectre Beam

`spectre_beam` is the external-channel boundary for Spectre applications. It
normalizes provider events, keeps endpoint and conversation affinity, runs
portable boundary pipelines, and delivers reactive or proactive messages with
idempotency.

Beam is made for Spectre, but it deliberately has **no runtime Mix dependency
on `:spectre`**. Its Stack manifest, Agent extension, action provider, identity
bridge, and Turn handler use Spectre's public contracts only when both
libraries are present. The Beam repository includes Spectre only in
`MIX_ENV=test`, so the complete integration remains covered.

The supported surface is listed in the
[public API manifest](docs/PUBLIC_API.md).

## Installation

Install Spectre from Hex and Beam from GitHub:

```elixir
def deps do
  [
    {:spectre, "~> 0.3.3"},
    {:spectre_beam, github: "elchemista/spectre_beam", branch: "main"}
  ]
end
```

Beam itself declares Spectre only for its integration suite:

```elixir
{:spectre, "~> 0.3.3", only: :test}
```

## Zero-configuration local chat

For IEx and LiveView, adding Beam to the Agent is enough. Beam compiles an
implicit local channel and starts the Agent's supervised gateway on the first
call:

```elixir
defmodule MyApp.SupportAgent do
  use Spectre.Agent
  use Spectre.Beam

  flow :support do
    on :question do
      reply :support_reply
    end
  end
end
```

From IEx, either call Beam directly or import the shell helpers:

```elixir
iex> Spectre.Beam.ask(MyApp.SupportAgent, "quanti ticket sono aperti?")
{:ok, "..."}

iex> import Spectre.Beam.IEx
iex> ask MyApp.SupportAgent, "quanti ticket sono aperti?"
{:ok, "..."}
iex> ask "e quali sono urgenti?"  # continues on the current conversation
{:ok, "..."}
```

In a LiveView, open by Agent module; no gateway child is required:

```elixir
def mount(%{"id" => id}, _session, socket) do
  {:ok, ref} = Spectre.Beam.Chat.open(MyApp.SupportAgent, conversation: id)
  if connected?(socket), do: Spectre.Beam.Chat.subscribe(ref)
  {:ok, assign(socket, beam_ref: ref)}
end

def handle_event("send", %{"text" => text}, socket) do
  {:ok, _ref} = Spectre.Beam.Chat.send(socket.assigns.beam_ref, text)
  {:noreply, socket}
end
```

Declare channels normally when providers are needed. If a process should be
transport-only and must not expose a local channel, use
`use Spectre.Beam, local: false`. An explicitly supervised
`Spectre.Beam.Gateway` remains available for custom names, sockets, stores,
clients, and production lifecycle control.

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

## Custom channel adapters: REST and MCP

`Spectre.Beam.Gateway` is transport-neutral. A custom transport implements the
`Spectre.Beam.Channel` behaviour; Beam continues to provide conversation
serialization, pipelines, idempotency, typing, throttling, retry, and the
bounded outbox around that adapter.

### REST API channel

For a REST integration, `decode/2` turns the webhook body into an inbound and
`deliver/2` sends the eventual reply through the remote API:

```elixir
defmodule MyApp.BeamAdapters.Rest do
  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.{Content, Inbound, Receipt}

  @impl true
  def capabilities(_opts), do: [:text]

  @impl true
  def decode(payload, _opts) do
    {:ok,
     Inbound.new(%{
       message_id: payload["id"],
       conversation_id: payload["conversation_id"],
       sender: payload["user_id"],
       content: Content.text(payload["text"]),
       metadata: %{transport: :rest}
     })}
  end

  @impl true
  def deliver(outbound, opts) do
    client = Keyword.fetch!(opts, :client)

    case client.post_message(%{
           conversation_id: outbound.conversation_id,
           reply_to: outbound.reply_to,
           text: outbound.content.text
         }) do
      {:ok, response} ->
        {:ok,
         Receipt.accepted(outbound,
           provider_message_id: response.id,
           metadata: %{transport: :rest}
         )}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def typing(user_id, composing?, opts) do
    opts
    |> Keyword.fetch!(:client)
    |> then(& &1.set_typing(user_id, composing?))
  end
end
```

Mount it like any bundled provider:

```elixir
install Spectre.Beam do
  channel :rest,
    type: :rest,
    adapter: MyApp.BeamAdapters.Rest,
    client: {MyApp.RemoteAPI, :new, []},
    typing: true,
    throttle: [messages_per_second: 10, burst: 20],
    retry: [max_attempts: 3, base_delay_ms: 250]
end
```

The HTTP controller is only responsible for authentication and ingress:

```elixir
def create(conn, payload) do
  case Spectre.Beam.Gateway.ingest(MyApp.SupportAgent, :rest, payload) do
    {:ok, ref} -> json(conn, %{accepted: true, conversation: Spectre.Beam.Ref.slug(ref)})
    {:duplicate, _ref} -> send_resp(conn, 202, "")
    :ignore -> send_resp(conn, 204, "")
    {:error, reason} -> json(conn, %{error: inspect(reason)})
  end
end
```

Start the Agent gateway during application boot when the REST endpoint must be
available before any local IEx or LiveView call:

```elixir
{:ok, MyApp.SupportAgent} = Spectre.Beam.Gateway.ensure(MyApp.SupportAgent)
```

### MCP as a synchronous tool

If MCP exposes only a synchronous `ask_agent` tool, it does not need a channel
adapter. The tool handler can use the local Chat surface directly:

```elixir
def ask_agent(%{"conversation_id" => id, "text" => text}) do
  with {:ok, ref} <-
         Spectre.Beam.Chat.open(MyApp.SupportAgent, conversation: id),
       {:ok, reply} <- Spectre.Beam.Chat.ask(ref, text, timeout: 60_000) do
    {:ok, %{"text" => reply, "conversation_id" => id}}
  end
end
```

This is appropriate when one MCP request remains open until one complete
answer is available.

### MCP as a conversational channel

Use an MCP channel adapter when MCP sessions should behave like Telegram,
WhatsApp, or any other long-lived transport: requests enter asynchronously,
conversations remain serialized, and replies or progress notifications are
sent later through the same MCP session.

```elixir
defmodule MyApp.BeamAdapters.MCP do
  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.{Content, Gateway, Inbound, Receipt}

  @impl true
  def capabilities(_opts), do: [:text]

  @impl true
  def decode(request, _opts) do
    {:ok,
     Inbound.new(%{
       # Beam copies this value to outbound.reply_to, providing correlation
       # between the asynchronous delivery and the original MCP request.
       message_id: request.id,
       conversation_id: request.session_id,
       sender: request.client_id,
       content: Content.text(request.params["text"]),
       metadata: %{transport: :mcp}
     })}
  end

  @impl true
  def deliver(outbound, opts) do
    server = Keyword.fetch!(opts, :client)

    case server.respond(outbound.reply_to, %{"text" => outbound.content.text}) do
      :ok ->
        {:ok,
         Receipt.accepted(outbound,
           provider_message_id: to_string(outbound.reply_to),
           metadata: %{transport: :mcp}
         )}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def typing(client_id, composing?, opts) do
    # Map this to a progress or notification primitive when the selected MCP
    # transport provides one. Typing is best effort in Beam.
    opts
    |> Keyword.fetch!(:client)
    |> then(& &1.progress(client_id, composing?))
  end

  @impl true
  def subscribe(opts) do
    server = Keyword.fetch!(opts, :client)
    gateway = Keyword.fetch!(opts, :gateway)
    endpoint = Keyword.fetch!(opts, :endpoint)

    server.subscribe(fn request ->
      Gateway.ingest(gateway, endpoint, request)
    end)
  end

  @impl true
  def unsubscribe(opts) do
    opts
    |> Keyword.fetch!(:client)
    |> then(& &1.unsubscribe())
  end
end
```

The Stack declaration retains the same delivery controls used by ExGram and
ExWapp:

```elixir
install Spectre.Beam do
  channel :mcp,
    type: :mcp,
    adapter: MyApp.BeamAdapters.MCP,
    client: {MyApp.MCPServer, :session, []},
    ingress: :subscribe,
    typing: true,
    throttle: [messages_per_second: 20, per_conversation: [messages_per_minute: 120]],
    retry: [max_attempts: 3],
    max_queue: 1_000,
    overflow: :reject
end
```

The MCP server must keep the original request pending, or retain an equivalent
session/request correlation, until `deliver/2` answers it. Beam preserves the
inbound `message_id` as `outbound.reply_to`, so the adapter does not need a
parallel correlation mechanism.

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

## Gateway runtime

Everything above is a boundary expressed as functions: the caller owns the
provider client, the concurrency, and the lifecycle. `Spectre.Beam.Gateway`
adds the process plane on top, so an application declares its channels once
and then talks to agents from anywhere in the node.

```elixir
children = [
  {Spectre.Supervisor, name: MyApp.SpectreSupervisor},
  {Spectre.Beam.Gateway,
   name: MyApp.Gateway,
   agent: MyApp.Agent,
   supervisor: MyApp.SpectreSupervisor,
   channels: [
     telegram: [
       type: :telegram,
       adapter: Spectre.Beam.Adapters.ExGram,
       client: {MyApp.Telegram, :session, []},
       ingress: :subscribe,
       coalesce_ms: 800,
       typing: true
     ],
     web: [type: :web, adapter: Spectre.Beam.Adapters.Local],
     console: [type: :console, adapter: Spectre.Beam.Adapters.Local]
   ],
   control: [socket: "/run/beam/gateway.sock"]}
]
```

The gateway adds four things the function boundary cannot express:

- **One process per conversation.** Two messages arriving at once on the same
  chat no longer run overlapping turns. Inbound idempotency deduplicates the
  *same* message; it does not order two different ones.
- **A resolved client.** `client:` is resolved once by the endpoint process —
  from a value, a zero-arity function, or an `{module, function, args}` — so
  no caller has to carry it.
- **Asynchronous delivery.** Replies go through a bounded per-endpoint outbox,
  which is what makes reply delay, throttling and retries safe to apply
  without blocking a LiveView or a webhook.
- **An event bus.** Every surface observes the same `Spectre.Beam.Event`
  values, each stamped with a monotonic per-conversation `seq`.

Provider events enter through one door, whatever the transport:

```elixir
{:ok, ref} = Spectre.Beam.Gateway.ingest(MyApp.Gateway, :telegram, raw_update)
{:ok, ref} = Spectre.Beam.Gateway.push(MyApp.Gateway, "telegram:12345", "il report è pronto")
```

A gateway declared without an `agent:` runs transport-only: inbound events are
normalized and published, and Spectre is never called. That keeps Beam usable
as a pure channel router.

## Talking to an agent from IEx

Local surfaces are not a second API — they are channels. The console, a
LiveView, the CLI and the test suite all go through
`Spectre.Beam.Adapters.Local`, so the pipelines, claims, throttling and policy
that guard Telegram guard them identically.

```elixir
# .iex.exs
import Spectre.Beam.IEx
```

```
iex> endpoints()
telegram      subscribe up          12 events     2026-08-20T15:04:11Z
console       none      up          3 events      2026-08-20T15:09:02Z

iex> say "quanti ticket aperti abbiamo?"
Al momento 14 ticket aperti, 3 con SLA in scadenza.

iex> say "telegram:12345", "ciao, tutto ok?"
iex> ls()
iex> tail "telegram:12345"
iex> cancel()
iex> doctor()
```

`say/1` addresses the *current* conversation, remembered in the shell process;
the first call opens one, and `focus/1` points the helpers elsewhere. For an
actual back-and-forth, `chat/0` takes over the shell:

```
iex> chat()
beam · console:a7f2 · MyApp.Agent · scope session
/help  /new  /who  /history  /stop  /endpoint ID  /exit

you › quali deploy sono usciti oggi?
bot › Tre: api@14:02, worker@15:10 e web@16:44.

you › /exit
```

With distribution enabled, `iex --remsh` gives the same helpers against a
running production node — which is full access to that node, so treat the
shell as the credential it is.

## LiveView and other OTP callers

`Spectre.Beam.Chat` is the whole surface. `send/3` returns as soon as the
message is queued and answers arrive as messages, so a slow model never blocks
the process handling a click.

```elixir
alias Spectre.Beam.{Chat, Event}

def mount(%{"id" => id}, _session, socket) do
  {:ok, ref} = Chat.open(MyApp.Gateway, "web:" <> id)
  if connected?(socket), do: Chat.subscribe(ref)

  {:ok,
   socket
   |> assign(ref: ref, typing?: false)
   |> stream(:messages, Chat.history(ref, limit: 50))}
end

def handle_event("send", %{"text" => text}, socket) do
  {:ok, _ref} = Chat.send(socket.assigns.ref, text)
  {:noreply, socket}
end

def handle_event("stop", _params, socket) do
  :ok = Chat.cancel(socket.assigns.ref)
  {:noreply, socket}
end

def handle_info(%Event{type: :inbound, payload: msg}, socket),
  do: {:noreply, stream_insert(socket, :messages, msg)}

def handle_info(%Event{type: :typing, payload: %{composing?: t}}, socket),
  do: {:noreply, assign(socket, typing?: t)}

def handle_info(%Event{type: :reply, payload: msg}, socket),
  do: {:noreply, stream_insert(socket, :messages, msg)}

def handle_info(%Event{type: :error, payload: payload}, socket),
  do: {:noreply, put_flash(socket, :error, inspect(payload.reason))}
```

Every event carries a `seq`. After a reconnect, ask for what was missed with
`Chat.history(ref, after: last_seq)` rather than replaying the whole
transcript.

The event types are a closed, versioned set: `:inbound`, `:typing`, `:delta`,
`:reply`, `:receipt`, `:policy_required`, `:action`, `:status`, `:error`.

## Local control socket and CLI

`control: [socket: path]` exposes the gateway on a Unix domain socket with
length-framed JSON, so a CLI, an editor plugin, or a client in any language can
drive it without joining the cluster.

```
$ mix beam.status
$ mix beam.send console:support "quanti ticket aperti?"
$ mix beam.send telegram:12345 "il report è pronto" --push
$ mix beam.tail telegram:12345
$ mix beam.chat --socket /run/beam/gateway.sock
$ mix beam.doctor
```

Without `--socket` the tasks start this project's application and talk to a
gateway in the same VM; with it they attach to a gateway already running
elsewhere — a release in production — over its control socket, loading none of
that node's code.

The socket is a full capability on the agent: anything that can write to it can
send messages as the gateway. It is created `0600` and owned by the running
user. Do not widen its mode to share it, and do not place it in a
world-writable directory.

## Testing a gateway

`Spectre.Beam.Adapters.Test` is a real channel, so a test drives the same code
production runs.

```elixir
setup do
  start_supervised!(
    {Spectre.Beam.Gateway,
     name: :test_gateway,
     agent: MyApp.Agent,
     channels: [chat: [type: :test, adapter: Spectre.Beam.Adapters.Test]]}
  )

  :ok = Spectre.Beam.Adapters.Test.attach(:test_gateway, :chat)
end

test "answers a question" do
  {:ok, _ref} = Spectre.Beam.Adapters.Test.send_inbound(:test_gateway, :chat, "question")
  assert {:ok, outbound} = Spectre.Beam.Adapters.Test.next_delivery()
  assert outbound.content.text =~ "risposta"
end
```

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
