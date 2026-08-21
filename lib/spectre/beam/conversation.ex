defmodule Spectre.Beam.Conversation do
  @moduledoc """
  One process per `{endpoint, conversation}` pair.

  This is the correctness centre of the gateway. Without it two messages
  arriving concurrently on the same chat run two overlapping turns against the
  same agent state — inbound idempotency deduplicates the *same* message, not
  two different ones. A conversation process makes its mailbox the queue, so
  turns on one chat are serial while different chats stay fully parallel.

  Owning that queue is what makes four further behaviours expressible:

    * **Coalescing** — several messages sent in quick succession become one
      turn instead of a burst of interleaved answers. Off by default; a chat
      channel usually wants a few hundred milliseconds.
    * **Cancellation** — `cancel/2` kills the running turn without disturbing
      the queue behind it.
    * **Typing lifecycle** — `:typing` events bracket the actual turn instead
      of a UI guessing from a timer.
    * **Replay** — a bounded transcript of stamped events lets a surface that
      reconnects ask for what it missed.

  The turn itself runs in a supervised task, so `status/2`, `history/3` and
  `cancel/2` answer immediately while a model call is in flight.
  """

  use GenServer, restart: :temporary

  alias Spectre.Beam.Bus
  alias Spectre.Beam.Content
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway.Spec
  alias Spectre.Beam.Identity
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Outbox
  alias Spectre.Beam.Ref
  alias Spectre.Beam.Runtime
  alias Spectre.Beam.Sequence
  alias Spectre.Beam.Telemetry

  @registry Spectre.Beam.Registry
  @task_supervisor Spectre.Beam.TaskSupervisor
  @spectre_supervisor :"Elixir.Spectre.Supervisor"

  @doc false
  @spec child_spec({Spec.t(), Ref.t()}) :: Supervisor.child_spec()
  def child_spec({%Spec{} = spec, %Ref{} = ref}) do
    %{
      id: {__MODULE__, Ref.key(ref)},
      start: {__MODULE__, :start_link, [{spec, ref}]},
      restart: :temporary,
      type: :worker
    }
  end

  @spec start_link({Spec.t(), Ref.t()}) :: GenServer.on_start()
  def start_link({%Spec{} = spec, %Ref{} = ref}) do
    GenServer.start_link(__MODULE__, {spec, ref}, name: name(ref))
  end

  @doc "Returns the via-tuple naming a conversation."
  @spec name(Ref.t()) :: GenServer.name()
  def name(%Ref{} = ref), do: {:via, Registry, {@registry, Ref.key(ref)}}

  @doc "Returns the pid of a live conversation."
  @spec whereis(Ref.t()) :: pid() | nil
  def whereis(%Ref{} = ref) do
    case Registry.lookup(@registry, Ref.key(ref)) do
      [{pid, _value}] -> pid
      [] -> nil
    end
  end

  @doc "Hands one normalized inbound to its conversation."
  @spec ingest(pid() | Ref.t(), Inbound.t(), term()) :: :ok
  def ingest(target, %Inbound{} = inbound, claim_key \\ nil) do
    GenServer.cast(server(target), {:ingest, inbound, claim_key})
  end

  @doc "Returns the observable state of a conversation."
  @spec status(Ref.t()) :: {:ok, map()} | {:error, :not_found}
  def status(%Ref{} = ref) do
    {:ok, GenServer.call(name(ref), :status)}
  catch
    :exit, _reason -> {:error, :not_found}
  end

  @doc """
  Returns retained events, oldest first.

    * `:after` — return only events stamped past this sequence number;
    * `:limit` — return at most this many of the most recent events.
  """
  @spec history(Ref.t(), keyword()) :: [Event.t()]
  def history(%Ref{} = ref, opts \\ []) do
    GenServer.call(name(ref), {:history, opts})
  catch
    :exit, _reason -> []
  end

  @doc "Stops the turn currently in flight, keeping queued messages."
  @spec cancel(Ref.t()) :: :ok
  def cancel(%Ref{} = ref) do
    GenServer.call(name(ref), :cancel)
  catch
    :exit, _reason -> :ok
  end

  @doc "Publishes one event on this conversation's topics."
  @spec publish(Ref.t(), Event.type(), map()) :: :ok
  def publish(%Ref{} = ref, type, payload) do
    GenServer.cast(server(ref), {:publish, type, payload})
  end

  @impl GenServer
  def init({spec, ref}) do
    # Trapping exits is what makes `terminate/2` run on an orderly shutdown,
    # which is where the sequence counter and the registry entry are released.
    # Without it a closed conversation leaves both behind.
    Process.flag(:trap_exit, true)

    case Spec.channel(spec, ref.endpoint) do
      {:error, reason} ->
        {:stop, {:invalid_beam_conversation, Ref.slug(ref), reason}}

      {:ok, channel} ->
        start(spec, ref, channel)
    end
  end

  @spec start(Spec.t(), Ref.t(), Spec.channel()) :: {:ok, map()}
  defp start(spec, ref, channel) do
    state = %{
      spec: spec,
      ref: ref,
      channel: channel,
      agent: Spec.agent_for(spec, ref.endpoint),
      target: nil,
      target_monitor: nil,
      status: :idle,
      task: nil,
      current: nil,
      claims: [],
      pending: :queue.new(),
      pending_count: 0,
      coalesce_timer: nil,
      transcript: :queue.new(),
      transcript_count: 0,
      idle_timer: nil,
      turns: 0
    }

    Telemetry.emit([:spectre, :beam, :conversation, :start], %{count: 1}, %{
      gateway: spec.name,
      endpoint: ref.endpoint
    })

    {:ok, arm_idle(state)}
  end

  @impl GenServer
  def handle_cast({:ingest, inbound, claim_key}, state) do
    state =
      state
      |> arm_idle()
      |> emit(:inbound, inbound_payload(inbound))
      |> enqueue(inbound, claim_key)
      |> maybe_start()

    {:noreply, state}
  end

  def handle_cast({:publish, type, payload}, state) do
    {:noreply, emit(state, type, payload)}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    {:reply,
     %{
       ref: state.ref,
       agent: state.agent,
       scope: state.channel.scope,
       status: state.status,
       pending: state.pending_count,
       turns: state.turns,
       seq: Sequence.current(Ref.topic(state.ref)),
       transcript: state.transcript_count
     }, state}
  end

  def handle_call({:history, opts}, _from, state) do
    {:reply, retained(state, opts), state}
  end

  def handle_call(:cancel, _from, %{task: nil} = state), do: {:reply, :ok, state}

  def handle_call(:cancel, _from, state) do
    _exit = Task.shutdown(state.task, :brutal_kill)

    state =
      state
      |> release_claims()
      |> emit(:status, %{status: :cancelled})
      |> Map.merge(%{task: nil, current: nil, status: :idle})
      |> emit_typing(false)
      |> maybe_start()

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:coalesce, state) do
    state = %{state | coalesce_timer: nil}

    case state.status do
      :idle -> {:noreply, start_turn(state)}
      _running -> {:noreply, state}
    end
  end

  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, finish_turn(state, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    {:noreply, finish_turn(state, {:error, {:beam_turn_crashed, reason}})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{target_monitor: ref} = state) do
    {:noreply, %{state | target: nil, target_monitor: nil}}
  end

  def handle_info(:idle_timeout, %{status: :idle, pending_count: 0} = state) do
    {:stop, :normal, state}
  end

  def handle_info(:idle_timeout, state), do: {:noreply, arm_idle(state)}

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    Registry.unregister(@registry, Ref.key(state.ref))
    Sequence.forget(Ref.topic(state.ref))

    Telemetry.emit([:spectre, :beam, :conversation, :stop], %{count: 1}, %{
      gateway: state.spec.name,
      endpoint: state.ref.endpoint,
      turns: state.turns
    })

    :ok
  end

  @spec enqueue(map(), Inbound.t(), term()) :: map()
  defp enqueue(state, inbound, claim_key) do
    if state.pending_count >= state.channel.max_pending do
      release_claim(state, claim_key)

      emit(state, :error, %{
        stage: :ingest,
        reason: {:beam_conversation_backlog_full, state.channel.max_pending}
      })
    else
      %{
        state
        | pending: :queue.in({inbound, claim_key}, state.pending),
          pending_count: state.pending_count + 1
      }
    end
  end

  @spec maybe_start(map()) :: map()
  defp maybe_start(%{status: {:running, _started_at}} = state), do: state
  defp maybe_start(%{pending_count: 0} = state), do: state

  defp maybe_start(%{coalesce_timer: timer} = state) when not is_nil(timer), do: state

  defp maybe_start(%{channel: %{coalesce_ms: window}} = state) when window > 0 do
    %{state | coalesce_timer: Process.send_after(self(), :coalesce, window)}
  end

  defp maybe_start(state), do: start_turn(state)

  @spec start_turn(map()) :: map()
  defp start_turn(state) do
    {batch, state} = drain_pending(state)

    case batch do
      [] -> state
      batch -> run_batch(state, batch)
    end
  end

  # A gateway without an agent is a router, not a broken agent: the inbound
  # events it published are the whole outcome, so their claims are completed
  # rather than released for a retry that would do the same thing again.
  @spec run_batch(map(), [{Inbound.t(), term()}]) :: map()
  defp run_batch(%{agent: nil} = state, batch) do
    state
    |> complete_claims(claim_keys(batch))
    |> emit(:status, %{status: :transport_only})
  end

  defp run_batch(state, batch) do
    {inbounds, claims} = Enum.unzip(batch)
    inbound = merge(inbounds)

    case resolve_target(state, inbound) do
      {:ok, state, target} ->
        dispatch_turn(state, target, inbound, Enum.reject(claims, &is_nil/1))

      {:error, reason} ->
        state
        |> release_claims(Enum.reject(claims, &is_nil/1))
        |> emit(:error, %{stage: :turn, reason: reason})
        |> maybe_start()
    end
  end

  # The input is built here rather than inside the task: a malformed inbound is
  # a boundary failure, and reporting it without ever marking the conversation
  # busy keeps the queue moving.
  @spec dispatch_turn(map(), term(), Inbound.t(), [term()]) :: map()
  defp dispatch_turn(state, target, inbound, claims) do
    case build_input(inbound) do
      {:ok, input} ->
        spawn_turn(state, target, input, inbound, claims)

      {:error, reason} ->
        state
        |> release_claims(claims)
        |> emit(:error, %{stage: :input, reason: reason})
        |> maybe_start()
    end
  end

  @spec spawn_turn(map(), term(), term(), Inbound.t(), [term()]) :: map()
  defp spawn_turn(state, target, input, inbound, claims) do
    turn_opts =
      state.channel.turn_opts
      |> Keyword.merge(state.spec.turn_opts)
      |> Keyword.put(:conversation_id, Inbound.conversation_key(inbound))

    metadata = %{gateway: state.spec.name, endpoint: state.ref.endpoint}

    task =
      Task.Supervisor.async_nolink(@task_supervisor, fn ->
        Telemetry.span(:turn, metadata, fn -> Runtime.turn(target, input, turn_opts) end)
      end)

    state
    |> Map.merge(%{task: task, current: inbound, claims: claims, status: {:running, now()}})
    |> emit(:status, %{status: :running})
    |> emit_typing(true)
  end

  @spec build_input(Inbound.t()) :: {:ok, term()} | {:error, term()}
  defp build_input(inbound) do
    {:ok, Runtime.to_input(inbound)}
  rescue
    exception -> {:error, {:invalid_beam_input, Exception.message(exception)}}
  end

  @spec finish_turn(map(), term()) :: map()
  defp finish_turn(state, result) do
    inbound = state.current

    state =
      state
      |> Map.merge(%{task: nil, current: nil, status: :idle, turns: state.turns + 1})
      |> emit_typing(false)
      |> handle_result(result, inbound)
      |> emit(:status, %{status: :idle})

    state
    |> arm_idle()
    |> maybe_start()
  end

  @spec handle_result(map(), term(), Inbound.t() | nil) :: map()
  defp handle_result(state, {:ok, turn}, %Inbound{} = inbound) when is_map(turn) do
    state = complete_claims(state)

    case Runtime.observable_reply(turn, inbound) do
      {:ok, text, idempotency_key} ->
        send_reply(state, inbound, text, idempotency_key, turn)

      :none ->
        emit(state, :status, %{status: :silent, turn: turn_summary(turn)})

      {:error, reason} ->
        emit(state, :error, %{stage: :reply, reason: reason})
    end
  end

  defp handle_result(state, {:error, reason}, _inbound) do
    state
    |> release_claims()
    |> emit(:error, %{stage: :turn, reason: reason})
  end

  defp handle_result(state, other, _inbound) do
    state
    |> release_claims()
    |> emit(:error, %{stage: :turn, reason: {:invalid_spectre_turn_reply, other}})
  end

  @spec send_reply(map(), Inbound.t(), String.t(), String.t(), map()) :: map()
  defp send_reply(state, inbound, text, idempotency_key, turn) do
    outbound =
      Outbound.new(%{
        endpoint: state.ref.endpoint,
        conversation_id: inbound.conversation_id,
        to: inbound.sender,
        reply_to: inbound.message_id,
        content: Content.text(text),
        idempotency_key: idempotency_key,
        metadata: %{kind: :reactive}
      })

    state = emit(state, :reply, %{text: text, outbound: outbound, turn: turn_summary(turn)})

    case Outbox.enqueue(state.spec.name, state.ref.endpoint, outbound, ref: state.ref) do
      :ok -> state
      {:error, reason} -> emit(state, :error, %{stage: :outbox, reason: reason})
    end
  rescue
    exception in ArgumentError ->
      emit(state, :error, %{stage: :reply, reason: {:invalid_beam_outbound, exception.message}})
  end

  @spec resolve_target(map(), Inbound.t()) :: {:ok, map(), term()} | {:error, term()}
  defp resolve_target(%{target: target} = state, _inbound) when not is_nil(target),
    do: {:ok, state, target}

  defp resolve_target(state, inbound) do
    cond do
      not Runtime.spectre_available?() ->
        {:error, :spectre_not_available}

      state.channel.scope == :instance ->
        start_instance(state, inbound)

      state.channel.session? ->
        start_session(state)

      true ->
        {:ok, state, state.agent}
    end
  end

  @spec start_instance(map(), Inbound.t()) :: {:ok, map(), pid()} | {:error, term()}
  defp start_instance(%{spec: %{supervisor: nil}}, _inbound),
    do: {:error, :beam_instance_supervisor_required}

  defp start_instance(state, inbound) do
    case Identity.resolve_instance(state.spec.supervisor, state.agent, inbound, []) do
      {:ok, pid} -> {:ok, monitor_target(state, pid), pid}
      {:error, _reason} = error -> error
    end
  end

  @spec start_session(map()) :: {:ok, map(), pid()} | {:error, term()}
  defp start_session(%{spec: %{supervisor: nil}} = state), do: {:ok, state, state.agent}

  defp start_session(state) do
    opts = [agent: state.agent, conversation_id: Ref.key(state.ref)]

    # Spectre is intentionally late-bound and absent from Beam's runtime deps.
    # credo:disable-for-next-line Credo.Check.Refactor.Apply
    case apply(@spectre_supervisor, :summon, [state.spec.supervisor, state.agent, opts]) do
      {:ok, pid} -> {:ok, monitor_target(state, pid), pid}
      {:ok, pid, _info} -> {:ok, monitor_target(state, pid), pid}
      {:error, {:already_started, pid}} -> {:ok, monitor_target(state, pid), pid}
      {:error, reason} -> {:error, {:beam_session_start_failed, reason}}
    end
  rescue
    exception -> {:error, {:beam_session_start_failed, Exception.message(exception)}}
  end

  @spec monitor_target(map(), pid()) :: map()
  defp monitor_target(state, pid) do
    %{state | target: pid, target_monitor: Process.monitor(pid)}
  end

  # With coalescing on, everything waiting becomes one turn. With it off, each
  # message keeps its own turn: messages that queued behind a slow answer must
  # not be silently merged into it.
  @spec drain_pending(map()) :: {[{Inbound.t(), term()}], map()}
  defp drain_pending(%{channel: %{coalesce_ms: window}} = state) when window > 0 do
    {:queue.to_list(state.pending), %{state | pending: :queue.new(), pending_count: 0}}
  end

  defp drain_pending(state) do
    case :queue.out(state.pending) do
      {{:value, item}, queue} ->
        {[item], %{state | pending: queue, pending_count: state.pending_count - 1}}

      {:empty, _queue} ->
        {[], state}
    end
  end

  # Coalesced messages answer once. The last inbound carries the reply target
  # and message id; the texts are joined in arrival order so nothing said is
  # dropped from the turn.
  @spec merge([Inbound.t()]) :: Inbound.t()
  defp merge([inbound]), do: inbound

  defp merge(inbounds) do
    last = List.last(inbounds)

    text =
      inbounds
      |> Enum.map(&(&1.content.text || ""))
      |> Enum.reject(&(&1 == ""))
      |> Enum.join("\n")

    %{
      last
      | content: %{last.content | text: text},
        metadata: Map.put(last.metadata, :coalesced, length(inbounds))
    }
  end

  @spec emit(map(), Event.type(), map()) :: map()
  defp emit(state, type, payload) do
    event = Bus.publish(state.spec.bus, Event.new(type, state.ref, payload))
    retain(state, event)
  end

  @spec emit_typing(map(), boolean()) :: map()
  defp emit_typing(state, composing?), do: emit(state, :typing, %{composing?: composing?})

  @spec retain(map(), Event.t()) :: map()
  defp retain(state, event) do
    transcript = :queue.in(event, state.transcript)
    count = state.transcript_count + 1

    if count > state.channel.transcript_limit do
      {_dropped, transcript} = :queue.out(transcript)
      %{state | transcript: transcript, transcript_count: count - 1}
    else
      %{state | transcript: transcript, transcript_count: count}
    end
  end

  @spec retained(map(), keyword()) :: [Event.t()]
  defp retained(state, opts) do
    events = :queue.to_list(state.transcript)

    events =
      case Keyword.get(opts, :after) do
        seq when is_integer(seq) -> Enum.filter(events, &(&1.seq > seq))
        _none -> events
      end

    events =
      case Keyword.get(opts, :types) do
        types when is_list(types) -> Enum.filter(events, &(&1.type in types))
        _none -> events
      end

    case Keyword.get(opts, :limit) do
      limit when is_integer(limit) and limit > 0 -> Enum.take(events, -limit)
      _none -> events
    end
  end

  @spec claim_keys([{Inbound.t(), term()}]) :: [term()]
  defp claim_keys(batch) do
    batch
    |> Enum.map(&elem(&1, 1))
    |> Enum.reject(&is_nil/1)
  end

  @spec complete_claims(map(), [term()] | nil) :: map()
  defp complete_claims(state, claims \\ nil) do
    Enum.each(
      claims || state.claims,
      &Spectre.Beam.Gateway.complete_claim(state.spec, &1, state.ref)
    )

    %{state | claims: []}
  end

  @spec release_claims(map(), [term()] | nil) :: map()
  defp release_claims(state, claims \\ nil) do
    Enum.each(claims || state.claims, &Spectre.Beam.Gateway.release_claim(state.spec, &1))
    %{state | claims: []}
  end

  @spec release_claim(map(), term()) :: :ok
  defp release_claim(_state, nil), do: :ok
  defp release_claim(state, claim), do: Spectre.Beam.Gateway.release_claim(state.spec, claim)

  @spec arm_idle(map()) :: map()
  defp arm_idle(state) do
    if state.idle_timer, do: Process.cancel_timer(state.idle_timer)

    case state.channel.idle_timeout_ms do
      timeout when is_integer(timeout) and timeout > 0 ->
        %{state | idle_timer: Process.send_after(self(), :idle_timeout, timeout)}

      _disabled ->
        %{state | idle_timer: nil}
    end
  end

  @spec inbound_payload(Inbound.t()) :: map()
  defp inbound_payload(inbound) do
    %{
      text: inbound.content.text,
      message_id: inbound.message_id,
      sender: inbound.sender,
      authenticated?: inbound.authenticated?,
      inbound: inbound
    }
  end

  @spec turn_summary(map()) :: map()
  defp turn_summary(turn) do
    %{decision: summarize(Map.get(turn, :decision)), observable: Map.get(turn, :observable)}
  end

  @spec summarize(term()) :: term()
  defp summarize({tag, _payload}) when is_atom(tag), do: tag
  defp summarize(other), do: other

  @spec server(pid() | Ref.t()) :: pid() | GenServer.name()
  defp server(pid) when is_pid(pid), do: pid
  defp server(%Ref{} = ref), do: name(ref)

  @spec now() :: integer()
  defp now, do: System.monotonic_time(:millisecond)
end
