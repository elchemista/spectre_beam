defmodule Spectre.Beam.Runtime do
  @moduledoc """
  High-level and primitive Beam runtime operations.
  """

  alias Spectre.Beam.Config
  alias Spectre.Beam.Content
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Exchange
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Pipeline
  alias Spectre.Beam.Receipt
  alias Spectre.Beam.Store
  alias Spectre.Input
  alias Spectre.Input.Source
  alias Spectre.Result
  alias Spectre.Turn

  @spec decode(module(), term(), term(), keyword()) ::
          {:ok, Inbound.t()} | :ignore | {:error, term()}
  def decode(agent, endpoint_id, event, opts \\ [])
      when is_atom(agent) and is_list(opts) do
    with {:ok, endpoint} <- endpoint(agent, endpoint_id),
         :ok <- validate_event_size(event, endpoint, opts),
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

  @spec to_input(Inbound.t()) :: Input.t()
  def to_input(%Inbound{} = inbound) do
    content = inbound.content

    %Input{
      text: content.text || "",
      raw: inbound,
      meta: %{},
      source: %Source{
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
  end

  @spec reply(module(), Inbound.t(), Result.t() | Turn.t(), keyword()) ::
          {:ok, Receipt.t() | nil} | {:error, term()}
  def reply(agent, %Inbound{} = inbound, %Turn{result: result}, opts),
    do: reply(agent, inbound, result, opts)

  def reply(agent, %Inbound{} = inbound, %Result{} = result, opts) when is_list(opts) do
    if Result.visible_reply?(result) do
      with {:ok, endpoint} <- endpoint(agent, inbound.endpoint) do
        outbound =
          Outbound.new(%{
            endpoint: endpoint.id,
            conversation_id: inbound.conversation_id,
            to: inbound.sender,
            reply_to: inbound.message_id,
            content: Content.text(result.reply_text),
            idempotency_key: reply_key(result, inbound),
            metadata: %{kind: :reactive}
          })

        deliver(endpoint, outbound, Keyword.put(opts, :agent, agent))
      end
    else
      {:ok, nil}
    end
  end

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

  @doc """
  Subscribes the calling process through an endpoint adapter, when supported.
  """
  @spec subscribe(module(), term(), keyword()) :: :ok | {:error, term()}
  def subscribe(agent, endpoint_id, opts \\ []) when is_atom(agent) and is_list(opts) do
    with {:ok, endpoint} <- endpoint(agent, endpoint_id) do
      call_lifecycle(endpoint, :subscribe, opts)
    end
  end

  @doc """
  Removes a subscription through an endpoint adapter, when supported.
  """
  @spec unsubscribe(module(), term(), keyword()) :: :ok | {:error, term()}
  def unsubscribe(agent, endpoint_id, opts \\ []) when is_atom(agent) and is_list(opts) do
    with {:ok, endpoint} <- endpoint(agent, endpoint_id) do
      call_lifecycle(endpoint, :unsubscribe, opts)
    end
  end

  @doc false
  @spec deliver(Endpoint.t(), Outbound.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, term()}
  def deliver(%Endpoint{} = endpoint, %Outbound{} = outbound, opts \\ []) do
    store = store(endpoint, :outbound, opts)
    key = {:outbound, endpoint.id, outbound.idempotency_key}

    case store_call(store, :claim, [key]) do
      :ok ->
        deliver_claimed(endpoint, outbound, opts, store, key)

      {:duplicate, %Receipt{} = receipt} ->
        {:ok, receipt}

      :in_progress ->
        {:error, {:beam_delivery_in_progress, outbound.idempotency_key}}

      {:error, _reason} = error ->
        error
    end
  end

  @spec handle_inbound(
          module() | GenServer.server(),
          module(),
          Inbound.t(),
          keyword()
        ) :: {:ok, Exchange.t()} | {:error, term()}
  defp handle_inbound(agent_or_session, agent, inbound, opts) do
    with {:ok, endpoint} <- endpoint(agent, inbound.endpoint) do
      store = store(endpoint, :inbound, opts)
      key = {:inbound, Inbound.key(inbound)}

      case store_call(store, :claim, [key]) do
        :ok ->
          run_claimed_turn(agent_or_session, agent, endpoint, inbound, opts, store, key)

        {:duplicate, %Exchange{} = exchange} ->
          resume_exchange(agent, endpoint, %{exchange | duplicate?: true}, opts, store, key)

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
          {module(), keyword()},
          term()
        ) :: {:ok, Exchange.t()} | {:error, term()}
  defp run_claimed_turn(agent_or_session, agent, endpoint, inbound, opts, store, key) do
    input = to_input(inbound)

    turn_opts =
      opts
      |> Keyword.put(:conversation_id, Inbound.conversation_key(inbound))
      |> Keyword.delete(:adapter_opts)

    case Spectre.turn(agent_or_session, input, turn_opts) do
      {:ok, %Turn{} = turn} ->
        exchange = %Exchange{inbound: inbound, input: input, turn: turn}

        with :ok <- store_call(store, :complete, [key, exchange]) do
          resume_exchange(agent, endpoint, exchange, opts, store, key)
        end

      {:error, reason} ->
        _released = store_call(store, :release, [key])
        {:error, reason}
    end
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

  @spec deliver_claimed(
          Endpoint.t(),
          Outbound.t(),
          keyword(),
          {module(), keyword()},
          term()
        ) :: {:ok, Receipt.t()} | {:error, term()}
  defp deliver_claimed(endpoint, outbound, opts, store, key) do
    with {:ok, prepared} <- run_pipeline(endpoint, :before_deliver, outbound, opts),
         :ok <- validate_pipeline_outbound(prepared, outbound, endpoint),
         :ok <- validate_outbound_capability(endpoint, prepared),
         {:ok, receipt} <- call_deliver(endpoint, prepared, opts),
         {:ok, receipt} <- run_pipeline(endpoint, :after_deliver, receipt, opts),
         :ok <- validate_pipeline_receipt(receipt, prepared, endpoint),
         :ok <- store_call(store, :complete, [key, receipt]) do
      record_delivery(endpoint, receipt, opts)
      {:ok, receipt}
    else
      {:error, {:ambiguous, _reason}} = error ->
        error

      :ignore ->
        _released = store_call(store, :release, [key])
        {:error, {:beam_pipeline_ignored_delivery, endpoint.id}}

      {:error, reason} ->
        _released = store_call(store, :release, [key])
        {:error, reason}
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
        case endpoint.adapter.deliver(outbound, Endpoint.adapter_opts(endpoint, opts)) do
          {:ok, %Receipt{} = receipt} ->
            {:ok, normalize_receipt(receipt, endpoint, outbound)}

          {:ok, receipt} when is_map(receipt) ->
            {:ok, receipt |> Receipt.new() |> normalize_receipt(endpoint, outbound)}

          {:error, _reason} = error ->
            error

          other ->
            {:error, {:invalid_beam_delivery_reply, endpoint.id, other}}
        end
    end
  rescue
    exception -> {:error, {:ambiguous, {:beam_delivery_exception, exception.__struct__}}}
  catch
    kind, reason -> {:error, {:ambiguous, {:beam_delivery_failure, kind, reason}}}
  end

  @spec call_lifecycle(Endpoint.t(), :subscribe | :unsubscribe, keyword()) ::
          :ok | {:error, term()}
  defp call_lifecycle(endpoint, callback, opts) do
    cond do
      not Code.ensure_loaded?(endpoint.adapter) ->
        {:error, {:beam_adapter_not_loaded, endpoint.id, endpoint.adapter}}

      not function_exported?(endpoint.adapter, callback, 1) ->
        {:error, {:beam_adapter_lifecycle_not_supported, endpoint.id, callback}}

      true ->
        case apply(endpoint.adapter, callback, [Endpoint.adapter_opts(endpoint, opts)]) do
          :ok -> :ok
          {:error, _reason} = error -> error
          other -> {:error, {:invalid_beam_adapter_lifecycle_reply, endpoint.id, callback, other}}
        end
    end
  rescue
    exception ->
      {:error, {:beam_adapter_lifecycle_exception, endpoint.id, callback, exception.__struct__}}
  catch
    kind, reason ->
      {:error, {:beam_adapter_lifecycle_failure, endpoint.id, callback, kind, reason}}
  end

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
    {:ok, Spectre.Session.agent(session)}
  rescue
    _exception -> {:error, {:invalid_spectre_session, session}}
  catch
    _kind, _reason -> {:error, {:invalid_spectre_session, session}}
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
  defp validate_pipeline_inbound(%Inbound{endpoint: id, channel_type: type}, endpoint)
       when id == endpoint.id and type == endpoint.type,
       do: :ok

  defp validate_pipeline_inbound(inbound, endpoint),
    do: {:error, {:invalid_beam_inbound_pipeline_value, endpoint.id, inbound}}

  @spec validate_pipeline_outbound(term(), Outbound.t(), Endpoint.t()) ::
          :ok | {:error, term()}
  defp validate_pipeline_outbound(
         %Outbound{endpoint: endpoint_id, idempotency_key: key},
         %Outbound{idempotency_key: key},
         %Endpoint{id: endpoint_id}
       ),
       do: :ok

  defp validate_pipeline_outbound(outbound, _original, endpoint),
    do: {:error, {:invalid_beam_outbound_pipeline_value, endpoint.id, outbound}}

  @spec validate_pipeline_receipt(term(), Outbound.t(), Endpoint.t()) ::
          :ok | {:error, term()}
  defp validate_pipeline_receipt(
         %Receipt{endpoint: endpoint_id, outbound_id: outbound_id},
         %Outbound{idempotency_key: outbound_id},
         %Endpoint{id: endpoint_id}
       ),
       do: :ok

  defp validate_pipeline_receipt(receipt, _outbound, endpoint),
    do: {:error, {:invalid_beam_receipt_pipeline_value, endpoint.id, receipt}}

  @spec reply_key(Result.t(), Inbound.t()) :: String.t()
  defp reply_key(result, inbound) do
    turn_id = get_in(result.metadata, [:runtime_identity, :turn_id]) || Spectre.Identity.uuid7()

    digest =
      :crypto.hash(
        :sha256,
        :erlang.term_to_binary({turn_id, inbound.endpoint, inbound.message_id}, [:deterministic])
      )
      |> Base.url_encode64(padding: false)

    "beam-reply:" <> digest
  end

  @spec store(Endpoint.t(), :inbound | :outbound, keyword()) :: {module(), keyword()}
  defp store(endpoint, kind, opts) do
    configured =
      case kind do
        :inbound ->
          Keyword.get(opts, :deduplicator) || endpoint.metadata.deduplicator

        :outbound ->
          Keyword.get(opts, :idempotency_store) || endpoint.metadata.idempotency_store
      end

    case configured do
      nil -> {Store, []}
      module when is_atom(module) -> {module, []}
      {module, store_opts} when is_atom(module) and is_list(store_opts) -> {module, store_opts}
      invalid -> {__MODULE__.InvalidStore, [configured: invalid]}
    end
  end

  @spec store_call({module(), keyword()}, atom(), list()) :: term()
  defp store_call({module, opts}, callback, args) do
    if Code.ensure_loaded?(module) and function_exported?(module, callback, length(args) + 1) do
      apply(module, callback, args ++ [opts])
    else
      {:error, {:invalid_beam_idempotency_store, module, callback}}
    end
  rescue
    exception ->
      {:error, {:beam_idempotency_store_exception, module, callback, exception.__struct__}}
  catch
    kind, reason -> {:error, {:beam_idempotency_store_failure, module, callback, kind, reason}}
  end

  @spec record_delivery(Endpoint.t(), Receipt.t(), keyword()) :: :ok
  defp record_delivery(endpoint, receipt, opts) do
    case Keyword.get(opts, :agent) do
      agent when is_atom(agent) and not is_nil(agent) ->
        _result =
          Spectre.Journal.record(
            agent,
            :beam_delivery,
            %{endpoint: endpoint.id, status: receipt.status},
            opts
          )

        :ok

      _other ->
        :ok
    end
  end
end
