defmodule Spectre.Beam.Gateway do
  @moduledoc """
  Supervised runtime that turns the Beam channel boundary into a gateway.

  `Spectre.Beam` is a boundary expressed as functions: the caller owns the
  provider client, the concurrency, and the lifecycle. That stays true and
  keeps working. A gateway is the process plane on top of it — it owns the
  endpoints, serializes each conversation, and delivers through a bounded
  outbox — so an application declares its channels once and then talks to
  agents from anywhere in the node.

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
             coalesce_ms: 800
           ],
           web: [type: :web, adapter: Spectre.Beam.Adapters.Local]
         ]}
      ]

  Every transport enters through `ingest/4` and every surface observes through
  the bus, so a webhook, a LiveView, the IEx console and the local socket are
  the same code path with different adapters.

  ## Options

    * `:name` — required, the gateway's registered name.
    * `:agent` — the agent module answering by default. Without one the
      gateway runs transport-only: inbound events are published and never
      turned. A channel may override it with its own `:agent`.
    * `:supervisor` — a `Spectre.Supervisor` name, required for `scope:
      :instance` and for supervised sessions.
    * `:scope` — `:session` (default) or `:instance`. Instance scope resolves
      an authenticated principal to a Spectre Subject before every turn.
    * `:session` — start a supervised `Spectre.Session` per conversation
      instead of running stateless agent-module turns.
    * `:store` — the idempotency store. Defaults to a private
      `Spectre.Beam.Store.ETS` started by the gateway, which is bounded; an
      explicit value is used as given and must already be running.
    * `:bus` — event bus, defaults to `Spectre.Beam.Bus.Local`.
    * `:channels` — endpoint declarations. Every option
      `Spectre.Beam.Endpoint` understands is passed through; the gateway adds
      `:client`, `:ingress`, `:agent`, `:scope`, `:session`, `:coalesce_ms`,
      `:idle_timeout_ms`, `:transcript_limit`, `:max_pending`, `:max_queue`
      and `:overflow`.
    * `:beam` — reuse a `Spectre.Beam.Config` compiled elsewhere instead of
      declaring channels inline.
  """

  use Supervisor

  alias Spectre.Beam.Conversation
  alias Spectre.Beam.Endpoint.Server, as: EndpointServer
  alias Spectre.Beam.Endpoint.Supervisor, as: EndpointSupervisor
  alias Spectre.Beam.Gateway.Holder
  alias Spectre.Beam.Gateway.Spec
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Outbox
  alias Spectre.Beam.Ref
  alias Spectre.Beam.Runtime
  alias Spectre.Beam.Store

  @registry Spectre.Beam.Registry
  @gateway_supervisor Spectre.Beam.GatewaySupervisor

  @type target :: Ref.t() | String.t()

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.get(opts, :name, __MODULE__)},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    case Spec.new(put_default_store(opts)) do
      {:ok, spec} ->
        Supervisor.start_link(__MODULE__, {spec, opts}, name: supervisor_name(spec.name))

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Supervisor
  def init({spec, opts}) do
    children =
      [Holder.child_spec(spec)] ++
        store_children(spec, opts) ++
        [
          {DynamicSupervisor,
           strategy: :one_for_one, name: conversations_name(spec.name), max_restarts: 10},
          %{
            id: {__MODULE__, :endpoints, spec.name},
            start: {Supervisor, :start_link, [endpoint_children(spec), endpoint_sup_opts(spec)]},
            type: :supervisor
          }
        ] ++ control_children(spec)

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc "Returns the compiled specification of a running gateway."
  @spec spec(atom()) :: {:ok, Spec.t()} | {:error, :not_found}
  defdelegate spec(gateway), to: Holder, as: :fetch

  @doc "Returns every gateway running on this node."
  @spec list() :: [atom()]
  defdelegate list(), to: Holder

  @doc """
  Returns a running gateway, starting one lazily from an Agent's Beam mount.

  This is the zero-configuration path used by IEx and LiveView helpers. An
  explicitly supervised gateway with the same name always wins.
  """
  @spec ensure(atom(), keyword()) :: {:ok, atom()} | {:error, term()}
  def ensure(agent_or_gateway, opts \\ []) when is_atom(agent_or_gateway) and is_list(opts) do
    case spec(agent_or_gateway) do
      {:ok, _spec} -> {:ok, agent_or_gateway}
      {:error, :not_found} -> start_agent_gateway(agent_or_gateway, opts)
    end
  end

  @doc "Stops a lazily started Agent gateway. Explicit gateways remain owner-managed."
  @spec stop(atom()) :: :ok | {:error, :not_managed}
  def stop(gateway) when is_atom(gateway) do
    case Registry.lookup(@registry, {:gateway_supervisor, gateway}) do
      [{pid, _value}] -> stop_gateway(pid)
      [] -> :ok
    end
  end

  @spec stop_gateway(pid()) :: :ok | {:error, :not_managed}
  defp stop_gateway(pid) do
    case DynamicSupervisor.terminate_child(@gateway_supervisor, pid) do
      :ok -> :ok
      {:error, :not_found} -> {:error, :not_managed}
    end
  catch
    :exit, _reason -> {:error, :not_managed}
  end

  @doc """
  Normalizes one provider event and routes it to its conversation.

  This is the single entry point every transport uses: a webhook controller, a
  polling ingress, a socket client, a LiveView, and the test channel all call
  it. The reply is produced asynchronously and observed on the bus.

  Returns the conversation reference, `{:duplicate, ref}` when the provider
  redelivered a message already seen, `:ignore` when the adapter or a pipeline
  discarded the event, or an error.
  """
  @spec ingest(atom(), term(), term(), keyword()) ::
          {:ok, Ref.t()} | {:duplicate, Ref.t()} | :ignore | {:error, term()}
  def ingest(gateway, endpoint_id, event, opts \\ []) when is_list(opts) do
    with {:ok, spec} <- spec(gateway),
         {:ok, inbound} <- decode(spec, endpoint_id, event, opts) do
      route(spec, inbound, opts)
    end
  end

  @doc """
  Routes an already normalized inbound, skipping decode.

  Useful when a host has its own normalization, or when replaying.
  """
  @spec ingest_inbound(atom(), Inbound.t(), keyword()) ::
          {:ok, Ref.t()} | {:duplicate, Ref.t()} | {:error, term()}
  def ingest_inbound(gateway, %Inbound{} = inbound, opts \\ []) when is_list(opts) do
    with {:ok, spec} <- spec(gateway), do: route(spec, inbound, opts)
  end

  @doc """
  Normalizes one provider event without routing it.
  """
  @spec decode(atom() | Spec.t(), term(), term(), keyword()) ::
          {:ok, Inbound.t()} | :ignore | {:error, term()}
  def decode(gateway, endpoint_id, event, opts \\ [])

  def decode(gateway, endpoint_id, event, opts) when is_atom(gateway) do
    with {:ok, spec} <- spec(gateway), do: decode(spec, endpoint_id, event, opts)
  end

  def decode(%Spec{} = spec, endpoint_id, event, opts) do
    Runtime.decode(spec.beam, endpoint_id, event, merge_adapter_opts(spec, endpoint_id, opts))
  end

  @doc """
  Queues a proactive message and returns without waiting for the provider.

  An explicit `:idempotency_key` makes the send safe to retry; without one a
  unique key is generated, which is correct for a fresh notification but does
  not deduplicate a repeated call.
  """
  @spec push(atom(), target(), String.t() | Spectre.Beam.Content.t(), keyword()) ::
          {:ok, Ref.t()} | {:error, term()}
  def push(gateway, target, content, opts \\ []) do
    with {:ok, spec} <- spec(gateway),
         {:ok, ref} <- resolve(spec, target, opts),
         {:ok, outbound} <- build_outbound(ref, content, opts),
         :ok <- Outbox.enqueue(spec.name, ref.endpoint, outbound, ref: ref) do
      {:ok, ref}
    end
  end

  @doc """
  Delivers one outbound synchronously, in the calling process.

  This is the gateway-aware form of `Spectre.Beam.deliver/4`: the endpoint's
  resolved client and store are supplied automatically. Prefer `push/4` from
  anything that must stay responsive.
  """
  @spec deliver(atom(), term(), Outbound.t() | map() | keyword(), keyword()) ::
          {:ok, Spectre.Beam.Receipt.t()} | {:error, term()}
  def deliver(gateway, endpoint_id, outbound, opts \\ []) do
    with {:ok, spec} <- spec(gateway),
         {:ok, endpoint} <- Spec.endpoint(spec, endpoint_id),
         {:ok, normalized} <- normalize_outbound(outbound, endpoint) do
      Outbox.deliver_now(spec, gateway, endpoint_id, normalized, opts)
    end
  end

  @doc """
  Ensures a conversation process exists and returns its reference.
  """
  @spec open(atom(), target(), keyword()) :: {:ok, Ref.t()} | {:error, term()}
  def open(gateway, target, opts \\ []) do
    with {:ok, spec} <- spec(gateway),
         {:ok, ref} <- resolve(spec, target, opts),
         {:ok, _pid} <- ensure_conversation(spec, ref) do
      {:ok, ref}
    end
  end

  @doc "Returns the references of every live conversation."
  @spec conversations(atom()) :: [Ref.t()]
  def conversations(gateway) do
    Registry.select(@registry, [
      {{{:conversation, gateway, :"$1"}, :_, :_}, [], [:"$1"]}
    ])
    |> Enum.map(&Ref.parse!(&1, gateway: gateway))
  end

  @doc "Returns one endpoint's runtime status."
  @spec endpoints(atom()) :: [map()]
  def endpoints(gateway) do
    case spec(gateway) do
      {:ok, spec} -> Enum.map(Spec.endpoints(spec), &endpoint_status(gateway, &1))
      {:error, :not_found} -> []
    end
  end

  @spec endpoint_status(atom(), Spectre.Beam.Endpoint.t()) :: map()
  defp endpoint_status(gateway, endpoint) do
    case EndpointServer.status(gateway, endpoint.id) do
      {:ok, status} -> status
      {:error, :not_found} -> %{endpoint: endpoint.id, status: :down}
    end
  end

  @doc "Stops one conversation process, discarding its retained transcript."
  @spec close(atom(), target()) :: :ok
  def close(gateway, target) do
    with {:ok, spec} <- spec(gateway),
         {:ok, ref} <- resolve(spec, target, []),
         pid when is_pid(pid) <- Conversation.whereis(ref) do
      DynamicSupervisor.terminate_child(conversations_name(gateway), pid)
      :ok
    else
      _absent -> :ok
    end
  end

  @doc false
  @spec complete_claim(Spec.t(), term(), term()) :: :ok
  def complete_claim(_spec, nil, _value), do: :ok

  def complete_claim(%Spec{} = spec, key, value) do
    _result = store_call(spec, :complete, [key, value])
    :ok
  end

  @doc false
  @spec release_claim(Spec.t(), term()) :: :ok
  def release_claim(_spec, nil), do: :ok

  def release_claim(%Spec{} = spec, key) do
    _result = store_call(spec, :release, [key])
    :ok
  end

  @doc """
  Resolves any accepted address into the gateway's canonical reference.

  Every path — parsing `"telegram:42"`, decoding an inbound, reusing a
  reference from a caller — produces the same struct for the same
  conversation, so references compare equal and address one process.
  """
  @spec resolve(Spec.t(), target(), keyword()) :: {:ok, Ref.t()} | {:error, term()}
  def resolve(%Spec{} = spec, %Ref{} = ref, opts),
    do: canonical_ref(spec, ref.endpoint, ref.conversation_id, opts)

  def resolve(%Spec{} = spec, target, opts) when is_binary(target) do
    endpoints = spec |> Spec.endpoints() |> Enum.map(& &1.id)

    with {:ok, parsed} <- Ref.parse(target, gateway: spec.name, endpoints: endpoints) do
      canonical_ref(spec, parsed.endpoint, parsed.conversation_id, opts)
    end
  end

  def resolve(_spec, target, _opts), do: {:error, {:invalid_beam_ref, target}}

  @spec canonical_ref(Spec.t(), term(), term(), keyword()) :: {:ok, Ref.t()} | {:error, term()}
  defp canonical_ref(spec, endpoint_id, conversation_id, opts) do
    with {:ok, channel} <- Spec.channel(spec, endpoint_id) do
      {:ok,
       Ref.new(%{
         gateway: spec.name,
         endpoint: endpoint_id,
         conversation_id: conversation_id,
         agent: Keyword.get(opts, :agent) || Spec.agent_for(spec, endpoint_id),
         scope: Keyword.get(opts, :scope, channel.scope)
       })}
    end
  end

  @spec route(Spec.t(), Inbound.t(), keyword()) ::
          {:ok, Ref.t()} | {:duplicate, Ref.t()} | {:error, term()}
  defp route(spec, inbound, opts) do
    with {:ok, ref} <- canonical_ref(spec, inbound.endpoint, inbound.conversation_id, []) do
      case claim(spec, inbound, opts) do
        {:ok, claim_key} -> start_and_ingest(spec, ref, inbound, claim_key)
        :duplicate -> {:duplicate, ref}
        {:error, _reason} = error -> error
      end
    end
  end

  @spec start_and_ingest(Spec.t(), Ref.t(), Inbound.t(), term()) ::
          {:ok, Ref.t()} | {:error, term()}
  defp start_and_ingest(spec, ref, inbound, claim_key) do
    case ensure_conversation(spec, ref) do
      {:ok, pid} ->
        Conversation.ingest(pid, inbound, claim_key)
        EndpointServer.record_event(spec.name, inbound.endpoint)
        {:ok, ref}

      {:error, reason} ->
        release_claim(spec, claim_key)
        {:error, reason}
    end
  end

  @spec claim(Spec.t(), Inbound.t(), keyword()) ::
          {:ok, term()} | :duplicate | {:error, term()}
  defp claim(spec, inbound, opts) do
    if Keyword.get(opts, :deduplicate, true) do
      key = {:inbound, spec.name, Inbound.key(inbound)}

      case store_call(spec, :claim, [key]) do
        :ok -> {:ok, key}
        :in_progress -> :duplicate
        {:duplicate, _value} -> :duplicate
        {:error, _reason} = error -> error
      end
    else
      {:ok, nil}
    end
  end

  @spec ensure_conversation(Spec.t(), Ref.t()) :: {:ok, pid()} | {:error, term()}
  defp ensure_conversation(spec, ref) do
    case Conversation.whereis(ref) do
      pid when is_pid(pid) ->
        {:ok, pid}

      nil ->
        case DynamicSupervisor.start_child(
               conversations_name(spec.name),
               {Conversation, {spec, ref}}
             ) do
          {:ok, pid} -> {:ok, pid}
          {:ok, pid, _info} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, {:beam_conversation_start_failed, reason}}
        end
    end
  end

  @spec build_outbound(Ref.t(), term(), keyword()) :: {:ok, Outbound.t()} | {:error, term()}
  defp build_outbound(ref, content, opts) do
    {:ok,
     Outbound.new(%{
       endpoint: ref.endpoint,
       conversation_id: ref.conversation_id,
       to: Keyword.get(opts, :to, ref.conversation_id),
       reply_to: Keyword.get(opts, :reply_to),
       content: normalize_content(content),
       idempotency_key: Keyword.get(opts, :idempotency_key) || generated_key(),
       metadata: Keyword.get(opts, :metadata, %{kind: :proactive})
     })}
  rescue
    exception in ArgumentError ->
      {:error, {:invalid_beam_outbound, Exception.message(exception)}}
  end

  @spec normalize_content(term()) :: Spectre.Beam.Content.t()
  defp normalize_content(text) when is_binary(text), do: Spectre.Beam.Content.text(text)
  defp normalize_content(content), do: Spectre.Beam.Content.new(content)

  @spec normalize_outbound(term(), Spectre.Beam.Endpoint.t()) ::
          {:ok, Outbound.t()} | {:error, term()}
  defp normalize_outbound(%Outbound{} = outbound, _endpoint), do: {:ok, outbound}

  defp normalize_outbound(attrs, endpoint) when is_list(attrs) or is_map(attrs) do
    attrs = if is_list(attrs), do: Map.new(attrs), else: attrs
    {:ok, attrs |> Map.put(:endpoint, endpoint.id) |> Outbound.new()}
  rescue
    exception in ArgumentError ->
      {:error, {:invalid_beam_outbound, endpoint.id, Exception.message(exception)}}
  end

  defp normalize_outbound(attrs, endpoint),
    do: {:error, {:invalid_beam_outbound, endpoint.id, attrs}}

  @spec merge_adapter_opts(Spec.t(), term(), keyword()) :: keyword()
  defp merge_adapter_opts(spec, endpoint_id, opts) do
    adapter_opts =
      spec.name
      |> EndpointServer.adapter_opts(endpoint_id)
      |> Keyword.merge(Keyword.get(opts, :adapter_opts, []))

    Keyword.put(opts, :adapter_opts, adapter_opts)
  end

  @spec store_call(Spec.t(), atom(), list()) :: term()
  defp store_call(spec, callback, args) do
    {module, store_opts} = spec.store || {Store, []}

    if Code.ensure_loaded?(module) and function_exported?(module, callback, length(args) + 1) do
      apply(module, callback, args ++ [store_opts])
    else
      {:error, {:invalid_beam_idempotency_store, module, callback}}
    end
  rescue
    exception -> {:error, {:beam_idempotency_store_exception, exception.__struct__}}
  end

  @spec put_default_store(keyword()) :: keyword()
  defp put_default_store(opts) do
    case Keyword.fetch(opts, :store) do
      {:ok, _store} ->
        opts

      :error ->
        name = Module.concat(Keyword.get(opts, :name, __MODULE__), "IdempotencyStore")
        Keyword.put(opts, :store, {Spectre.Beam.Store.ETS, [name: name]})
    end
  end

  @spec start_agent_gateway(module(), keyword()) :: {:ok, atom()} | {:error, term()}
  defp start_agent_gateway(agent, opts) do
    with {:ok, beam} <- Spectre.Beam.config(agent) do
      gateway_opts =
        beam.options
        |> Keyword.merge(opts)
        |> Keyword.merge(name: agent, agent: agent, beam: beam)

      case DynamicSupervisor.start_child(@gateway_supervisor, {__MODULE__, gateway_opts}) do
        {:ok, _pid} -> {:ok, agent}
        {:ok, _pid, _info} -> {:ok, agent}
        {:error, {:already_started, _pid}} -> {:ok, agent}
        {:error, reason} -> {:error, {:beam_gateway_start_failed, agent, reason}}
      end
    end
  catch
    :exit, reason -> {:error, {:beam_gateway_supervisor_unavailable, reason}}
  end

  # Only a store the gateway itself defaulted to is supervised here. An
  # explicitly configured store belongs to whoever configured it, and starting
  # a second copy would silently split the idempotency state in two.
  @spec store_children(Spec.t(), keyword()) :: [Supervisor.child_spec() | {module(), term()}]
  defp store_children(spec, opts) do
    if Keyword.has_key?(opts, :store) do
      []
    else
      {module, store_opts} = spec.store
      [{module, store_opts}]
    end
  end

  # The control socket starts last: it must never accept a request before the
  # endpoints it would address are running.
  @spec control_children(Spec.t()) :: [Supervisor.child_spec() | {module(), term()}]
  defp control_children(%Spec{control: control} = spec) do
    if Keyword.has_key?(control, :socket) or Keyword.has_key?(control, :port),
      do: [{Spectre.Beam.Socket.Server, {spec.name, control}}],
      else: []
  end

  @spec endpoint_children(Spec.t()) :: [Supervisor.child_spec()]
  defp endpoint_children(spec) do
    Enum.map(Spec.endpoints(spec), &EndpointSupervisor.child_spec({spec, &1.id}))
  end

  @spec endpoint_sup_opts(Spec.t()) :: keyword()
  defp endpoint_sup_opts(spec) do
    [
      strategy: :one_for_one,
      name: {:via, Registry, {@registry, {:endpoints, spec.name}}}
    ]
  end

  @spec supervisor_name(atom()) :: GenServer.name()
  defp supervisor_name(name), do: {:via, Registry, {@registry, {:gateway_supervisor, name}}}

  @spec conversations_name(atom()) :: GenServer.name()
  defp conversations_name(name), do: {:via, Registry, {@registry, {:conversations, name}}}

  @spec generated_key() :: String.t()
  defp generated_key,
    do: "beam-push:" <> Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
end
