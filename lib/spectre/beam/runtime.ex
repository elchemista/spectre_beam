defmodule Spectre.Beam.Runtime do
  @moduledoc """
  Executes Beam provider boundaries and the optional Spectre turn bridge.

  All references to Spectre core are late-bound. The module therefore compiles
  and its provider primitives work when `:spectre` is not installed, while a
  host that installs both libraries gets the complete Agent integration.
  """

  alias Spectre.Beam.Config
  alias Spectre.Beam.Content
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Exchange
  alias Spectre.Beam.Identity
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Logistics
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Pipeline
  alias Spectre.Beam.Receipt
  alias Spectre.Beam.Store

  @spectre :"Elixir.Spectre"
  @agent_ref :"Elixir.Spectre.AgentRef"
  @identity :"Elixir.Spectre.Identity"
  @instance :"Elixir.Spectre.Instance"
  @journal :"Elixir.Spectre.Journal"
  @result :"Elixir.Spectre.Result"
  @run_ref :"Elixir.Spectre.Run.Ref"
  @session :"Elixir.Spectre.Session"
  @turn :"Elixir.Spectre.Turn"
  @input :"Elixir.Spectre.Input"

  @spec decode(Config.t() | module(), term(), term(), keyword()) ::
          {:ok, Inbound.t()} | :ignore | {:error, term()}
  def decode(config_or_agent, endpoint_id, event, opts \\ [])

  def decode(%Config{} = config, endpoint_id, event, opts) when is_list(opts) do
    with {:ok, endpoint} <- Config.fetch(config, endpoint_id) do
      decode_endpoint(endpoint, event, opts)
    end
  end

  def decode(agent, endpoint_id, event, opts) when is_atom(agent) and is_list(opts) do
    with {:ok, endpoint} <- endpoint(agent, endpoint_id) do
      decode_endpoint(endpoint, event, Keyword.put_new(opts, :agent, agent))
    end
  end

  @spec decode_endpoint(Endpoint.t(), term(), keyword()) ::
          {:ok, Inbound.t()} | :ignore | {:error, term()}
  defp decode_endpoint(endpoint, event, opts) do
    with :ok <- validate_event_size(event, endpoint, opts),
         {:ok, event} <- run_pipeline(endpoint, :before_decode, event, opts),
         {:ok, inbound} <- call_decode(endpoint, event, opts),
         {:ok, inbound} <- normalize_inbound(inbound, endpoint),
         {:ok, inbound} <- run_pipeline(endpoint, :after_decode, inbound, opts),
         :ok <- validate_pipeline_inbound(inbound, endpoint) do
      {:ok, inbound}
    else
      :ignore -> :ignore
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Completes the inbound boundary for an already normalized value.

  A gateway surface that builds its own inbound — a console line, a LiveView
  message, a socket frame, a replayed event — must still pass the endpoint's
  `:after_decode` pipeline, or locally injected messages would silently skip
  the enrichment and control plugs that guard the provider ones.
  """
  @spec finish_decode(Config.t(), term(), Inbound.t(), keyword()) ::
          {:ok, Inbound.t()} | :ignore | {:error, term()}
  def finish_decode(%Config{} = config, endpoint_id, %Inbound{} = inbound, opts \\ []) do
    with {:ok, endpoint} <- Config.fetch(config, endpoint_id),
         {:ok, inbound} <- normalize_inbound(inbound, endpoint),
         {:ok, inbound} <- run_pipeline(endpoint, :after_decode, inbound, opts),
         :ok <- validate_pipeline_inbound(inbound, endpoint) do
      {:ok, inbound}
    end
  end

  @doc """
  Runs one Spectre turn against an agent module, a Session, or an Instance.

  Late-bound like every other core call, so a gateway that runs without
  Spectre installed gets a stable `{:error, :spectre_not_available}` instead of
  an undefined function.
  """
  @spec turn(term(), term(), keyword()) :: {:ok, map()} | {:error, term()}
  def turn(target, input, opts \\ []) when is_list(opts) do
    core_call(@spectre, :turn, [target, input, opts])
  end

  @doc "Returns true when Spectre core is loaded in this runtime."
  @spec spectre_available?() :: boolean()
  def spectre_available?, do: Code.ensure_loaded?(@spectre)

  @doc false
  @spec to_input(Inbound.t()) :: term()
  def to_input(%Inbound{} = inbound) do
    content = inbound.content

    attrs = %{
      text: content.text || "",
      raw: inbound,
      meta: %{},
      source: %{
        kind: :beam,
        mount: inbound.endpoint,
        conversation_id: inbound.conversation_id,
        actor_id: inbound.sender,
        reply_to: inbound.message_id,
        metadata: %{
          channel_type: inbound.channel_type,
          authenticated?: inbound.authenticated?,
          modalities: Content.modalities(content)
        }
      }
    }

    if Code.ensure_loaded?(@input) and function_exported?(@input, :new, 1) do
      # Spectre is intentionally late-bound and absent from Beam's runtime deps.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      apply(@input, :new, [attrs])
    else
      raise ArgumentError, "Spectre is required to convert a Beam inbound into Spectre.Input"
    end
  end

  @spec reply(module(), Inbound.t(), term(), keyword()) ::
          {:ok, Receipt.t() | nil} | {:error, term()}
  def reply(agent, inbound, turn, opts \\ [])

  def reply(agent, %Inbound{} = inbound, turn, opts)
      when is_atom(agent) and is_map(turn) and is_list(opts) do
    case observable_reply(turn, inbound) do
      {:ok, output, idempotency_key} ->
        deliver_reply(agent, inbound, output, idempotency_key, opts)

      :none ->
        {:ok, nil}

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Returns the observable reply of a Spectre Turn without delivering it.

  `Spectre.Beam.reply/4` delivers inline, which is correct for a caller-owned
  boundary. A gateway conversation instead needs the reply text and its stable
  idempotency key so it can hand the outbound to an endpoint outbox and stay
  responsive while the provider is slow.

  Returns `:none` when the Turn produced nothing an external channel should
  see — a silent decision, a pending policy, an internal-only result.
  """
  @spec observable_reply(map(), Inbound.t()) ::
          {:ok, String.t(), String.t()} | :none | {:error, term()}
  def observable_reply(turn, %Inbound{} = inbound) when is_map(turn) do
    cond do
      core_struct?(turn, @turn) -> turn_reply(turn, inbound)
      core_struct?(turn, @result) -> {:error, :beam_turn_boundary_required}
      true -> {:error, {:invalid_beam_turn, turn}}
    end
  end

  def observable_reply(turn, _inbound), do: {:error, {:invalid_beam_turn, turn}}

  @spec turn_reply(map(), Inbound.t()) :: {:ok, String.t(), String.t()} | :none
  defp turn_reply(turn, inbound) do
    case Map.get(turn, :observable) do
      {:reply, output, ref} when is_binary(output) and is_map(ref) ->
        if core_struct?(ref, @run_ref), do: {:ok, output, reply_key(ref)}, else: :none

      nil ->
        legacy_observable(Map.get(turn, :decision), inbound)

      _other ->
        :none
    end
  end

  @spec legacy_observable(term(), Inbound.t()) :: {:ok, String.t(), String.t()} | :none
  defp legacy_observable({:reply, result}, inbound) when is_map(result) do
    text = Map.get(result, :reply_text)

    if is_binary(text) and core_struct?(result, @result) and visible_reply?(result),
      do: {:ok, text, legacy_reply_key(result, inbound)},
      else: :none
  end

  defp legacy_observable(_decision, _inbound), do: :none

  @spec handle(module() | GenServer.server(), term(), term(), keyword()) ::
          {:ok, Exchange.t()} | :ignore | {:error, term()}
  def handle(agent_or_session, endpoint_id, event, opts \\ []) when is_list(opts) do
    with {:ok, agent} <- agent_for(agent_or_session),
         {:ok, inbound} <- normalize_decode(decode(agent, endpoint_id, event, opts)) do
      handle_inbound(agent_or_session, agent, inbound, opts)
    else
      :ignore -> :ignore
      {:error, _reason} = error -> error
    end
  end

  @spec handle_instance(
          GenServer.server(),
          module() | map(),
          term(),
          term(),
          keyword()
        ) :: {:ok, Exchange.t()} | :ignore | {:error, term()}
  def handle_instance(supervisor, agent_or_ref, endpoint_id, event, opts \\ [])
      when is_list(opts) do
    with {:ok, agent} <- agent_definition(agent_or_ref),
         {:ok, inbound} <- normalize_decode(decode(agent, endpoint_id, event, opts)),
         {:ok, instance} <- Identity.resolve_instance(supervisor, agent_or_ref, inbound, opts),
         {:ok, instance_key} <- instance_key(instance) do
      scope = {:instance, instance_key}
      handle_inbound(instance, agent, inbound, opts, instance_turn_opts(opts), scope)
    else
      :ignore -> :ignore
      {:error, _reason} = error -> error
    end
  end

  @spec deliver(Config.t(), term(), Outbound.t() | map() | keyword(), keyword()) ::
          {:ok, Receipt.t()} | {:error, term()}
  def deliver(%Config{} = config, endpoint_id, outbound, opts) when is_list(opts) do
    with {:ok, endpoint} <- Config.fetch(config, endpoint_id),
         {:ok, outbound} <- normalize_outbound(outbound, endpoint) do
      deliver(endpoint, outbound, opts)
    end
  end

  def deliver(%Config{} = config, endpoint_id, outbound),
    do: deliver(config, endpoint_id, outbound, [])

  @doc false
  @spec deliver(Endpoint.t(), Outbound.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, term()}
  def deliver(%Endpoint{} = endpoint, %Outbound{} = outbound, opts) when is_list(opts) do
    store = store(endpoint, :outbound, opts)
    key = {:outbound, endpoint.id, outbound.idempotency_key}

    case store_call(store, :claim, [key]) do
      :ok ->
        deliver_claimed(endpoint, outbound, opts, store, key)

      {:duplicate, %Receipt{} = receipt} ->
        {:ok, receipt}

      {:duplicate, value} ->
        {:error, {:invalid_beam_idempotency_value, key, value}}

      :in_progress ->
        {:error, {:beam_delivery_in_progress, outbound.idempotency_key}}

      {:error, _reason} = error ->
        error
    end
  end

  @spec subscribe(Config.t() | module(), term(), keyword()) :: :ok | {:error, term()}
  def subscribe(config_or_agent, endpoint_id, opts \\ [])

  def subscribe(%Config{} = config, endpoint_id, opts) when is_list(opts) do
    with {:ok, endpoint} <- Config.fetch(config, endpoint_id),
         do: call_lifecycle(endpoint, :subscribe, opts)
  end

  def subscribe(agent, endpoint_id, opts) when is_atom(agent) and is_list(opts) do
    with {:ok, endpoint} <- endpoint(agent, endpoint_id),
         do: call_lifecycle(endpoint, :subscribe, opts)
  end

  @spec unsubscribe(Config.t() | module(), term(), keyword()) :: :ok | {:error, term()}
  def unsubscribe(config_or_agent, endpoint_id, opts \\ [])

  def unsubscribe(%Config{} = config, endpoint_id, opts) when is_list(opts) do
    with {:ok, endpoint} <- Config.fetch(config, endpoint_id),
         do: call_lifecycle(endpoint, :unsubscribe, opts)
  end

  def unsubscribe(agent, endpoint_id, opts) when is_atom(agent) and is_list(opts) do
    with {:ok, endpoint} <- endpoint(agent, endpoint_id),
         do: call_lifecycle(endpoint, :unsubscribe, opts)
  end

  @spec handle_inbound(module() | GenServer.server(), module(), Inbound.t(), keyword()) ::
          {:ok, Exchange.t()} | {:error, term()}
  defp handle_inbound(agent_or_session, agent, inbound, opts) do
    handle_inbound(agent_or_session, agent, inbound, opts, opts, nil)
  end

  @spec handle_inbound(
          module() | GenServer.server(),
          module(),
          Inbound.t(),
          keyword(),
          keyword(),
          term()
        ) :: {:ok, Exchange.t()} | {:error, term()}
  defp handle_inbound(agent_or_session, agent, inbound, opts, turn_opts, scope) do
    with {:ok, endpoint} <- endpoint(agent, inbound.endpoint) do
      store = store(endpoint, :inbound, opts)
      key = inbound_claim_key(inbound, scope)

      case store_call(store, :claim, [key]) do
        :ok ->
          run_claimed_turn(
            agent_or_session,
            agent,
            endpoint,
            inbound,
            opts,
            turn_opts,
            store,
            key
          )

        {:duplicate, %Exchange{} = exchange} ->
          resume_exchange(agent, endpoint, %{exchange | duplicate?: true}, opts, store, key)

        {:duplicate, value} ->
          {:error, {:invalid_beam_idempotency_value, key, value}}

        :in_progress ->
          {:error, {:duplicate_beam_inbound_in_progress, Inbound.key(inbound)}}

        {:error, _reason} = error ->
          error
      end
    end
  end

  @spec run_claimed_turn(
          module() | GenServer.server(),
          module(),
          Endpoint.t(),
          Inbound.t(),
          keyword(),
          keyword(),
          {module(), keyword()},
          term()
        ) :: {:ok, Exchange.t()} | {:error, term()}
  defp run_claimed_turn(
         agent_or_session,
         agent,
         endpoint,
         inbound,
         opts,
         turn_opts,
         store,
         key
       ) do
    input = to_input(inbound)

    turn_opts =
      turn_opts
      |> Keyword.put(:conversation_id, Inbound.conversation_key(inbound))
      |> Keyword.delete(:adapter_opts)

    case core_call(@spectre, :turn, [agent_or_session, input, turn_opts]) do
      {:ok, turn} when is_map(turn) ->
        persist_turn(turn, agent, endpoint, inbound, input, opts, store, key)

      {:error, reason} ->
        release_with_error(store, key, reason)

      other ->
        release_with_error(store, key, {:invalid_spectre_turn_reply, other})
    end
  end

  @spec persist_turn(
          map(),
          module(),
          Endpoint.t(),
          Inbound.t(),
          term(),
          keyword(),
          {module(), keyword()},
          term()
        ) :: {:ok, Exchange.t()} | {:error, term()}
  defp persist_turn(turn, agent, endpoint, inbound, input, opts, store, key) do
    if core_struct?(turn, @turn) do
      complete_turn_claim(turn, agent, endpoint, inbound, input, opts, store, key)
    else
      release_with_error(store, key, {:invalid_spectre_turn, turn})
    end
  end

  @spec complete_turn_claim(
          map(),
          module(),
          Endpoint.t(),
          Inbound.t(),
          term(),
          keyword(),
          {module(), keyword()},
          term()
        ) :: {:ok, Exchange.t()} | {:error, term()}
  defp complete_turn_claim(turn, agent, endpoint, inbound, input, opts, store, key) do
    exchange = %Exchange{inbound: inbound, input: input, turn: turn}

    with :ok <- store_call(store, :complete, [key, exchange]) do
      resume_exchange(agent, endpoint, exchange, opts, store, key)
    end
  end

  @spec inbound_claim_key(Inbound.t(), term()) :: term()
  defp inbound_claim_key(inbound, nil), do: {:inbound, Inbound.key(inbound)}

  defp inbound_claim_key(inbound, scope),
    do: {:inbound, scope, Inbound.conversation_key(inbound), Inbound.key(inbound)}

  @spec instance_turn_opts(keyword()) :: keyword()
  defp instance_turn_opts(opts) do
    Keyword.drop(opts, [
      :authenticated_at,
      :proof_ref,
      :identity_metadata,
      :subject_registry,
      :instance_registry,
      :instance_opts
    ])
  end

  @spec resume_exchange(
          module(),
          Endpoint.t(),
          Exchange.t(),
          keyword(),
          {module(), keyword()},
          term()
        ) :: {:ok, Exchange.t()} | {:error, term()}
  defp resume_exchange(
         _agent,
         _endpoint,
         %Exchange{receipt: %Receipt{}} = exchange,
         _opts,
         _store,
         _key
       ),
       do: {:ok, exchange}

  defp resume_exchange(agent, _endpoint, %Exchange{} = exchange, opts, store, key) do
    case reply(agent, exchange.inbound, exchange.turn, opts) do
      {:ok, receipt} ->
        completed = %{exchange | receipt: receipt}

        with :ok <- store_call(store, :complete, [key, completed]) do
          {:ok, completed}
        end

      {:error, _reason} = error ->
        error
    end
  end

  @spec deliver_claimed(Endpoint.t(), Outbound.t(), keyword(), {module(), keyword()}, term()) ::
          {:ok, Receipt.t()} | {:error, term()}
  defp deliver_claimed(endpoint, outbound, opts, store, key) do
    case prepare_delivery(endpoint, outbound, opts) do
      {:ok, prepared} ->
        dispatch_claimed(endpoint, prepared, opts, store, key)

      :ignore ->
        _released = store_call(store, :release, [key])
        {:error, {:beam_pipeline_ignored_delivery, endpoint.id}}

      {:error, reason} ->
        release_with_error(store, key, reason)
    end
  end

  @spec prepare_delivery(Endpoint.t(), Outbound.t(), keyword()) ::
          {:ok, Outbound.t()} | :ignore | {:error, term()}
  defp prepare_delivery(endpoint, outbound, opts) do
    with {:ok, prepared} <- run_pipeline(endpoint, :before_deliver, outbound, opts),
         :ok <- validate_pipeline_outbound(prepared, outbound, endpoint),
         :ok <- validate_outbound_capability(endpoint, prepared),
         :ok <- Logistics.before_deliver(endpoint, prepared, opts) do
      {:ok, prepared}
    end
  end

  @spec dispatch_claimed(
          Endpoint.t(),
          Outbound.t(),
          keyword(),
          {module(), keyword()},
          term()
        ) :: {:ok, Receipt.t()} | {:error, term()}
  defp dispatch_claimed(endpoint, outbound, opts, store, key) do
    case Logistics.deliver_with_retry(endpoint, opts, fn _attempt ->
           call_deliver(endpoint, outbound, opts)
         end) do
      {:ok, receipt} ->
        finalize_dispatched(endpoint, outbound, receipt, opts, store, key)

      {:error, {:ambiguous, _reason}} = error ->
        error

      {:error, reason} ->
        release_with_error(store, key, reason)
    end
  end

  # Once the adapter reports success, any receipt-pipeline or persistence
  # failure is ambiguous: the provider may already have delivered the message.
  # Keeping the claim fenced is safer than turning a bookkeeping failure into
  # a duplicate external side effect on retry.
  @spec finalize_dispatched(
          Endpoint.t(),
          Outbound.t(),
          Receipt.t(),
          keyword(),
          {module(), keyword()},
          term()
        ) :: {:ok, Receipt.t()} | {:error, term()}
  defp finalize_dispatched(endpoint, outbound, receipt, opts, store, key) do
    with {:ok, receipt} <- run_pipeline(endpoint, :after_deliver, receipt, opts),
         :ok <- validate_pipeline_receipt(receipt, outbound, endpoint),
         :ok <- store_call(store, :complete, [key, receipt]) do
      record_delivery(endpoint, receipt, opts)
      {:ok, receipt}
    else
      :ignore ->
        {:error, {:ambiguous, {:beam_pipeline_ignored_delivery, endpoint.id}}}

      {:error, {:ambiguous, _reason}} = error ->
        error

      {:error, reason} ->
        {:error, {:ambiguous, reason}}
    end
  end

  @spec call_decode(Endpoint.t(), term(), keyword()) ::
          {:ok, Inbound.t() | map()} | :ignore | {:error, term()}
  defp call_decode(endpoint, event, opts) do
    cond do
      not Code.ensure_loaded?(endpoint.adapter) ->
        {:error, {:beam_adapter_not_loaded, endpoint.id, endpoint.adapter}}

      not function_exported?(endpoint.adapter, :decode, 2) ->
        {:error, {:invalid_beam_adapter, endpoint.id, :decode}}

      true ->
        endpoint.adapter.decode(event, Endpoint.adapter_opts(endpoint, opts))
    end
  rescue
    exception -> {:error, {:beam_decode_exception, endpoint.id, exception.__struct__}}
  catch
    kind, reason -> {:error, {:beam_decode_failure, endpoint.id, kind, reason}}
  end

  @spec call_deliver(Endpoint.t(), Outbound.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, term()}
  defp call_deliver(endpoint, outbound, opts) do
    cond do
      not Code.ensure_loaded?(endpoint.adapter) ->
        {:error, {:beam_adapter_not_loaded, endpoint.id, endpoint.adapter}}

      not function_exported?(endpoint.adapter, :deliver, 2) ->
        {:error, {:invalid_beam_adapter, endpoint.id, :deliver}}

      true ->
        normalize_delivery_reply(
          endpoint.adapter.deliver(outbound, Endpoint.adapter_opts(endpoint, opts)),
          endpoint,
          outbound
        )
    end
  rescue
    exception -> {:error, {:ambiguous, {:beam_delivery_exception, exception.__struct__}}}
  catch
    kind, reason -> {:error, {:ambiguous, {:beam_delivery_failure, kind, reason}}}
  end

  @spec normalize_delivery_reply(term(), Endpoint.t(), Outbound.t()) ::
          {:ok, Receipt.t()} | {:error, term()}
  defp normalize_delivery_reply({:ok, %Receipt{} = receipt}, endpoint, outbound),
    do: {:ok, normalize_receipt(receipt, endpoint, outbound)}

  defp normalize_delivery_reply({:ok, receipt}, endpoint, outbound) when is_map(receipt) do
    {:ok, receipt |> Receipt.new() |> normalize_receipt(endpoint, outbound)}
  rescue
    exception -> {:error, {:invalid_beam_receipt, endpoint.id, Exception.message(exception)}}
  end

  defp normalize_delivery_reply({:error, _reason} = error, _endpoint, _outbound), do: error

  defp normalize_delivery_reply(other, endpoint, _outbound),
    do: {:error, {:invalid_beam_delivery_reply, endpoint.id, other}}

  @spec call_lifecycle(Endpoint.t(), :subscribe | :unsubscribe, keyword()) ::
          :ok | {:error, term()}
  defp call_lifecycle(endpoint, callback, opts) do
    cond do
      not Code.ensure_loaded?(endpoint.adapter) ->
        {:error, {:beam_adapter_not_loaded, endpoint.id, endpoint.adapter}}

      not function_exported?(endpoint.adapter, callback, 1) ->
        {:error, {:beam_adapter_lifecycle_not_supported, endpoint.id, callback}}

      true ->
        endpoint.adapter
        |> apply(callback, [Endpoint.adapter_opts(endpoint, opts)])
        |> normalize_lifecycle_reply(endpoint, callback)
    end
  rescue
    exception ->
      {:error, {:beam_adapter_lifecycle_exception, endpoint.id, callback, exception.__struct__}}
  catch
    kind, reason ->
      {:error, {:beam_adapter_lifecycle_failure, endpoint.id, callback, kind, reason}}
  end

  @spec normalize_lifecycle_reply(term(), Endpoint.t(), atom()) :: :ok | {:error, term()}
  defp normalize_lifecycle_reply(:ok, _endpoint, _callback), do: :ok
  defp normalize_lifecycle_reply({:error, _reason} = error, _endpoint, _callback), do: error

  defp normalize_lifecycle_reply(other, endpoint, callback),
    do: {:error, {:invalid_beam_adapter_lifecycle_reply, endpoint.id, callback, other}}

  @spec normalize_inbound(Inbound.t() | map(), Endpoint.t()) ::
          {:ok, Inbound.t()} | {:error, term()}
  defp normalize_inbound(inbound, endpoint) do
    inbound =
      inbound
      |> Inbound.new()
      |> Map.put(:endpoint, endpoint.id)
      |> Map.put(:channel_type, endpoint.type)

    {:ok, inbound}
  rescue
    exception -> {:error, {:invalid_beam_inbound, endpoint.id, Exception.message(exception)}}
  end

  @spec normalize_outbound(Outbound.t() | map() | keyword(), Endpoint.t()) ::
          {:ok, Outbound.t()} | {:error, term()}
  defp normalize_outbound(%Outbound{endpoint: endpoint_id} = outbound, %Endpoint{id: endpoint_id}),
       do: {:ok, outbound}

  defp normalize_outbound(%Outbound{} = outbound, endpoint),
    do: {:error, {:beam_outbound_endpoint_mismatch, endpoint.id, outbound.endpoint}}

  defp normalize_outbound(attrs, endpoint) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      attrs |> Map.new() |> normalize_outbound(endpoint)
    else
      {:error, {:invalid_beam_outbound, endpoint.id, attrs}}
    end
  end

  defp normalize_outbound(attrs, endpoint) when is_map(attrs) do
    {:ok, attrs |> Map.put(:endpoint, endpoint.id) |> Outbound.new()}
  rescue
    exception -> {:error, {:invalid_beam_outbound, endpoint.id, Exception.message(exception)}}
  end

  defp normalize_outbound(attrs, endpoint),
    do: {:error, {:invalid_beam_outbound, endpoint.id, attrs}}

  @spec normalize_receipt(Receipt.t(), Endpoint.t(), Outbound.t()) :: Receipt.t()
  defp normalize_receipt(receipt, endpoint, outbound) do
    %{
      receipt
      | endpoint: endpoint.id,
        outbound_id: receipt.outbound_id || outbound.idempotency_key,
        occurred_at: receipt.occurred_at || DateTime.utc_now()
    }
  end

  @spec normalize_decode({:ok, Inbound.t()} | :ignore | {:error, term()}) ::
          {:ok, Inbound.t()} | :ignore | {:error, term()}
  defp normalize_decode(result), do: result

  @spec endpoint(module(), term()) :: {:ok, Endpoint.t()} | {:error, term()}
  defp endpoint(agent, endpoint_id) do
    with {:ok, %Config{} = config} <- Spectre.Beam.config(agent) do
      Config.fetch(config, endpoint_id)
    end
  end

  @spec agent_for(module() | GenServer.server()) :: {:ok, module()} | {:error, term()}
  defp agent_for(agent) when is_atom(agent) do
    case Spectre.Beam.config(agent) do
      {:ok, _config} -> {:ok, agent}
      {:error, _reason} -> session_agent(agent)
    end
  end

  defp agent_for(session), do: session_agent(session)

  @spec session_agent(GenServer.server()) :: {:ok, module()} | {:error, term()}
  defp session_agent(session) do
    if Code.ensure_loaded?(@session) and function_exported?(@session, :agent, 1) do
      # Spectre is intentionally late-bound and absent from Beam's runtime deps.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      {:ok, apply(@session, :agent, [session])}
    else
      {:error, {:invalid_spectre_session, session}}
    end
  rescue
    _exception -> {:error, {:invalid_spectre_session, session}}
  catch
    _kind, _reason -> {:error, {:invalid_spectre_session, session}}
  end

  @spec agent_definition(module() | map()) :: {:ok, module()} | {:error, term()}
  defp agent_definition(%{__struct__: @agent_ref} = ref) do
    case core_call(@agent_ref, :validate, [ref]) do
      :ok -> {:ok, Map.fetch!(ref, :definition)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp agent_definition(agent) when is_atom(agent) and not is_nil(agent), do: {:ok, agent}
  defp agent_definition(agent), do: {:error, {:invalid_beam_agent_ref, agent}}

  @spec instance_key(pid()) :: {:ok, term()} | {:error, term()}
  defp instance_key(instance) do
    case core_call(@instance, :ref, [instance]) do
      ref when is_map(ref) -> {:ok, Map.fetch!(ref, :key)}
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_spectre_instance_ref, other}}
    end
  end

  @spec validate_outbound_capability(Endpoint.t(), Outbound.t()) :: :ok | {:error, term()}
  defp validate_outbound_capability(endpoint, outbound) do
    with {:ok, capabilities} <- Endpoint.capabilities(endpoint) do
      if MapSet.member?(capabilities, outbound.content.type),
        do: :ok,
        else: {:error, {:unsupported_beam_capability, endpoint.id, outbound.content.type}}
    end
  end

  @spec validate_event_size(term(), Endpoint.t(), keyword()) :: :ok | {:error, term()}
  defp validate_event_size(event, endpoint, opts) do
    max_bytes =
      Keyword.get(opts, :max_payload_bytes) ||
        endpoint.metadata.max_payload_bytes ||
        1_000_000

    bytes = :erlang.external_size(event)

    if is_integer(max_bytes) and max_bytes > 0 and bytes <= max_bytes,
      do: :ok,
      else: {:error, {:beam_payload_too_large, endpoint.id, bytes, max_bytes}}
  end

  @spec run_pipeline(Endpoint.t(), Pipeline.stage(), term(), keyword()) ::
          {:ok, term()} | :ignore | {:error, term()}
  defp run_pipeline(endpoint, stage, value, opts) do
    case Pipeline.run(stage, endpoint, value, Endpoint.pipeline(endpoint, stage), opts) do
      {:ok, transformed, _pipeline} ->
        {:ok, transformed}

      {:halt, :ignore, _pipeline} ->
        :ignore

      {:halt, {:error, reason}, _pipeline} ->
        {:error, reason}

      {:halt, reason, _pipeline} ->
        {:error, {:beam_pipeline_halted, endpoint.id, stage, reason}}

      {:error, _reason} = error ->
        error
    end
  end

  @spec validate_pipeline_inbound(term(), Endpoint.t()) :: :ok | {:error, term()}
  defp validate_pipeline_inbound(%Inbound{endpoint: id, channel_type: type} = inbound, endpoint)
       when id == endpoint.id and type == endpoint.type do
    validate_pipeline_struct(
      inbound,
      &Inbound.new/1,
      :invalid_beam_inbound_pipeline_value,
      endpoint
    )
  end

  defp validate_pipeline_inbound(inbound, endpoint),
    do: {:error, {:invalid_beam_inbound_pipeline_value, endpoint.id, inbound}}

  @spec validate_pipeline_outbound(term(), Outbound.t(), Endpoint.t()) ::
          :ok | {:error, term()}
  defp validate_pipeline_outbound(
         %Outbound{endpoint: endpoint_id, idempotency_key: key} = outbound,
         %Outbound{idempotency_key: key},
         %Endpoint{id: endpoint_id} = endpoint
       ) do
    validate_pipeline_struct(
      outbound,
      &Outbound.new/1,
      :invalid_beam_outbound_pipeline_value,
      endpoint
    )
  end

  defp validate_pipeline_outbound(outbound, _original, endpoint),
    do: {:error, {:invalid_beam_outbound_pipeline_value, endpoint.id, outbound}}

  @spec validate_pipeline_receipt(term(), Outbound.t(), Endpoint.t()) ::
          :ok | {:error, term()}
  defp validate_pipeline_receipt(
         %Receipt{endpoint: endpoint_id, outbound_id: outbound_id} = receipt,
         %Outbound{idempotency_key: outbound_id},
         %Endpoint{id: endpoint_id} = endpoint
       ) do
    validate_pipeline_struct(
      receipt,
      &Receipt.new/1,
      :invalid_beam_receipt_pipeline_value,
      endpoint
    )
  end

  defp validate_pipeline_receipt(receipt, _outbound, endpoint),
    do: {:error, {:invalid_beam_receipt_pipeline_value, endpoint.id, receipt}}

  @spec validate_pipeline_struct(term(), (term() -> term()), atom(), Endpoint.t()) ::
          :ok | {:error, term()}
  defp validate_pipeline_struct(value, validator, error, endpoint) do
    _validated = validator.(value)
    :ok
  rescue
    _exception -> {:error, {error, endpoint.id, value}}
  end

  @spec deliver_reply(module(), Inbound.t(), String.t(), String.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, term()}
  defp deliver_reply(agent, inbound, output, idempotency_key, opts) do
    with {:ok, endpoint} <- endpoint(agent, inbound.endpoint) do
      outbound =
        Outbound.new(%{
          endpoint: endpoint.id,
          conversation_id: inbound.conversation_id,
          to: inbound.sender,
          reply_to: inbound.message_id,
          content: Content.text(output),
          idempotency_key: idempotency_key,
          metadata: %{kind: :reactive}
        })

      deliver(endpoint, outbound, Keyword.put(opts, :agent, agent))
    end
  end

  @spec reply_key(map()) :: String.t()
  # Spectre is intentionally late-bound and absent from Beam's runtime deps.
  # credo:disable-for-next-line Credo.Check.Refactor.Apply
  defp reply_key(ref), do: "beam-reply:" <> apply(@run_ref, :token, [ref])

  @spec legacy_reply_key(map(), Inbound.t()) :: String.t()
  defp legacy_reply_key(result, inbound) do
    turn_id = get_in(result, [Access.key(:metadata, %{}), :runtime_identity, :turn_id]) || uuid7()

    digest =
      :crypto.hash(
        :sha256,
        :erlang.term_to_binary({turn_id, inbound.endpoint, inbound.message_id}, [:deterministic])
      )
      |> Base.url_encode64(padding: false)

    "beam-legacy-reply:" <> digest
  end

  @spec visible_reply?(map()) :: boolean()
  defp visible_reply?(result) do
    # Spectre is intentionally late-bound and absent from Beam's runtime deps.
    # credo:disable-for-lines:2 Credo.Check.Refactor.Apply
    Code.ensure_loaded?(@result) and function_exported?(@result, :visible_reply?, 1) and
      apply(@result, :visible_reply?, [result])
  end

  @spec uuid7() :: String.t()
  defp uuid7 do
    if Code.ensure_loaded?(@identity) and function_exported?(@identity, :uuid7, 0),
      # Spectre is intentionally late-bound and absent from Beam's runtime deps.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      do: apply(@identity, :uuid7, []),
      else: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
  end

  @spec store(Endpoint.t(), :inbound | :outbound, keyword()) :: {module(), keyword()}
  defp store(endpoint, kind, opts) do
    endpoint
    |> configured_store(kind, opts)
    |> normalize_store()
  end

  @spec configured_store(Endpoint.t(), :inbound | :outbound, keyword()) :: term()
  defp configured_store(endpoint, :inbound, opts),
    do: Keyword.get(opts, :deduplicator) || endpoint.metadata.deduplicator

  defp configured_store(endpoint, :outbound, opts),
    do: Keyword.get(opts, :idempotency_store) || endpoint.metadata.idempotency_store

  @spec normalize_store(term()) :: {module(), keyword()}
  defp normalize_store(nil), do: {Store, []}
  defp normalize_store(module) when is_atom(module), do: {module, []}

  defp normalize_store({module, store_opts}) when is_atom(module) and is_list(store_opts),
    do: {module, store_opts}

  defp normalize_store(invalid),
    do: {__MODULE__.InvalidStore, [configured: invalid]}

  @spec store_call({module(), keyword()}, atom(), list()) :: term()
  defp store_call({module, opts}, callback, args) do
    if Code.ensure_loaded?(module) and function_exported?(module, callback, length(args) + 1) do
      module
      |> apply(callback, args ++ [opts])
      |> normalize_store_reply(module, callback)
    else
      {:error, {:invalid_beam_idempotency_store, module, callback}}
    end
  rescue
    exception ->
      {:error, {:beam_idempotency_store_exception, module, callback, exception.__struct__}}
  catch
    kind, reason -> {:error, {:beam_idempotency_store_failure, module, callback, kind, reason}}
  end

  @spec normalize_store_reply(term(), module(), atom()) :: term()
  defp normalize_store_reply(reply, _module, :claim)
       when reply == :ok or reply == :in_progress,
       do: reply

  defp normalize_store_reply({:duplicate, _value} = reply, _module, :claim), do: reply
  defp normalize_store_reply({:error, _reason} = reply, _module, _callback), do: reply

  defp normalize_store_reply(:ok, _module, callback) when callback in [:complete, :release],
    do: :ok

  defp normalize_store_reply(reply, module, callback),
    do: {:error, {:invalid_beam_idempotency_store_reply, module, callback, reply}}

  @spec release_with_error({module(), keyword()}, term(), term()) :: {:error, term()}
  defp release_with_error(store, key, reason) do
    _released = store_call(store, :release, [key])
    {:error, reason}
  end

  @spec record_delivery(Endpoint.t(), Receipt.t(), keyword()) :: :ok
  defp record_delivery(endpoint, receipt, opts) do
    case Keyword.get(opts, :agent) do
      agent when is_atom(agent) and not is_nil(agent) ->
        if Code.ensure_loaded?(@journal) and function_exported?(@journal, :record, 4) do
          # Spectre is intentionally late-bound and absent from Beam's runtime deps.
          # credo:disable-for-lines:2 Credo.Check.Refactor.Apply
          _result =
            apply(@journal, :record, [
              agent,
              :beam_delivery,
              %{endpoint: endpoint.id, status: receipt.status},
              opts
            ])
        end

        :ok

      _other ->
        :ok
    end
  end

  @spec core_call(module(), atom(), list()) :: term()
  defp core_call(module, function, args) do
    if Code.ensure_loaded?(module) and function_exported?(module, function, length(args)) do
      apply(module, function, args)
    else
      {:error, :spectre_not_available}
    end
  end

  @spec core_struct?(term(), module()) :: boolean()
  defp core_struct?(%{__struct__: module}, module), do: true
  defp core_struct?(_value, _module), do: false
end
