defmodule Spectre.Beam.Outbox do
  @moduledoc """
  Per-endpoint delivery queue.

  `Spectre.Beam.deliver/4` blocks its caller for the whole reply delay,
  throttle reservation and retry budget. That is correct for a caller-owned
  boundary and wrong for a gateway: a LiveView event handler, a webhook
  controller, or a conversation waiting to accept the next message must not
  wait on a provider.

  The outbox accepts an outbound, answers immediately, and delivers in a
  supervised task. Pacing is preserved because deliveries on one endpoint run
  serially by default — which is what a provider rate limit wants anyway.

  The queue is bounded. A provider that stops accepting messages produces a
  declared, observable failure rather than a process that grows until the node
  dies: `:reject` (default) refuses new work, `:drop_oldest` sheds the head.
  Either way an `:error` event is published on the bus.
  """

  use GenServer

  alias Spectre.Beam.Bus
  alias Spectre.Beam.Endpoint.Server, as: EndpointServer
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway.Spec
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Ref
  alias Spectre.Beam.Runtime
  alias Spectre.Beam.Telemetry

  @registry Spectre.Beam.Registry
  @task_supervisor Spectre.Beam.TaskSupervisor
  @default_concurrency 1

  @doc false
  @spec child_spec({Spec.t(), term()}) :: Supervisor.child_spec()
  def child_spec({%Spec{} = spec, id}) do
    %{
      id: {__MODULE__, spec.name, id},
      start: {__MODULE__, :start_link, [{spec, id}]},
      type: :worker
    }
  end

  @spec start_link({Spec.t(), term()}) :: GenServer.on_start()
  def start_link({%Spec{} = spec, id}) do
    GenServer.start_link(__MODULE__, {spec, id}, name: name(spec.name, id))
  end

  @doc "Returns the via-tuple naming an endpoint outbox."
  @spec name(atom(), term()) :: GenServer.name()
  def name(gateway, id), do: {:via, Registry, {@registry, {:outbox, gateway, id}}}

  @doc """
  Queues one outbound for delivery and returns without waiting for it.

  `:ref` in `opts` associates the delivery with a conversation, so its receipt
  or failure is published where subscribers are watching.
  """
  @spec enqueue(atom(), term(), Outbound.t(), keyword()) :: :ok | {:error, term()}
  def enqueue(gateway, id, %Outbound{} = outbound, opts \\ []) do
    GenServer.call(name(gateway, id), {:enqueue, outbound, opts})
  catch
    :exit, _reason -> {:error, {:beam_outbox_unavailable, id}}
  end

  @doc "Returns queue depth and in-flight count."
  @spec info(atom(), term()) :: {:ok, map()} | {:error, :not_found}
  def info(gateway, id) do
    {:ok, GenServer.call(name(gateway, id), :info)}
  catch
    :exit, _reason -> {:error, :not_found}
  end

  @doc """
  Waits until the queue is empty and nothing is in flight.

  Intended for tests and for an orderly shutdown.
  """
  @spec drain(atom(), term(), timeout()) :: :ok | {:error, :timeout}
  def drain(gateway, id, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_drain(gateway, id, deadline)
  end

  @impl GenServer
  def init({spec, id}) do
    case Spec.channel(spec, id) do
      {:error, reason} ->
        {:stop, {:invalid_beam_endpoint, id, reason}}

      {:ok, channel} ->
        {:ok,
         %{
           gateway: spec.name,
           id: id,
           spec: spec,
           channel: channel,
           queue: :queue.new(),
           queued: 0,
           tasks: %{},
           max_queue: channel.max_queue,
           overflow: channel.overflow,
           concurrency: @default_concurrency,
           delivered: 0,
           failed: 0
         }}
    end
  end

  @impl GenServer
  def handle_call({:enqueue, outbound, opts}, _from, state) do
    case admit(state, outbound, opts) do
      {:ok, state} -> {:reply, :ok, dispatch(state)}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:info, _from, state) do
    {:reply,
     %{
       gateway: state.gateway,
       endpoint: state.id,
       queued: state.queued,
       inflight: map_size(state.tasks),
       delivered: state.delivered,
       failed: state.failed,
       max_queue: state.max_queue,
       overflow: state.overflow
     }, state}
  end

  @impl GenServer
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {item, tasks} ->
        Process.demonitor(ref, [:flush])

        state =
          %{state | tasks: tasks}
          |> report(item, result)
          |> dispatch()

        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {item, tasks} ->
        state =
          %{state | tasks: tasks}
          |> report(item, {:error, {:beam_delivery_crashed, reason}})
          |> dispatch()

        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @spec admit(map(), Outbound.t(), keyword()) :: {:ok, map()} | {:error, term(), map()}
  defp admit(state, outbound, opts) do
    item = %{outbound: outbound, opts: opts, ref: Keyword.get(opts, :ref)}

    cond do
      state.queued < state.max_queue ->
        {:ok, %{state | queue: :queue.in(item, state.queue), queued: state.queued + 1}}

      state.overflow == :drop_oldest ->
        {{:value, dropped}, queue} = :queue.out(state.queue)
        state = report(%{state | queue: queue, queued: state.queued - 1}, dropped, overflow())

        {:ok, %{state | queue: :queue.in(item, state.queue), queued: state.queued + 1}}

      true ->
        {:error, {:beam_outbox_full, state.id, state.max_queue}, state}
    end
  end

  @spec overflow() :: {:error, term()}
  defp overflow, do: {:error, :beam_outbox_overflow}

  @spec dispatch(map()) :: map()
  defp dispatch(state) do
    if map_size(state.tasks) < state.concurrency and state.queued > 0 do
      {{:value, item}, queue} = :queue.out(state.queue)
      task = start_delivery(state, item)

      dispatch(%{
        state
        | queue: queue,
          queued: state.queued - 1,
          tasks: Map.put(state.tasks, task.ref, item)
      })
    else
      state
    end
  end

  @spec start_delivery(map(), map()) :: Task.t()
  defp start_delivery(state, item) do
    spec = state.spec
    gateway = state.gateway
    id = state.id
    outbound = item.outbound
    opts = item.opts

    Task.Supervisor.async_nolink(@task_supervisor, fn ->
      Telemetry.span(:deliver, %{gateway: gateway, endpoint: id}, fn ->
        deliver_now(spec, gateway, id, outbound, opts)
      end)
    end)
  end

  @doc false
  @spec deliver_now(Spec.t(), atom(), term(), Outbound.t(), keyword()) ::
          {:ok, Spectre.Beam.Receipt.t()} | {:error, term()}
  def deliver_now(%Spec{} = spec, gateway, id, %Outbound{} = outbound, opts) do
    with {:ok, endpoint} <- Spec.endpoint(spec, id) do
      Runtime.deliver(endpoint, outbound, delivery_opts(spec, gateway, id, opts))
    end
  end

  @spec delivery_opts(Spec.t(), atom(), term(), keyword()) :: keyword()
  defp delivery_opts(spec, gateway, id, opts) do
    adapter_opts =
      gateway
      |> EndpointServer.adapter_opts(id)
      |> Keyword.merge(Keyword.get(opts, :adapter_opts, []))

    opts
    |> Keyword.drop([:ref])
    |> Keyword.put(:adapter_opts, adapter_opts)
    |> put_agent(spec, id)
    |> put_store(spec)
  end

  @spec put_agent(keyword(), Spec.t(), term()) :: keyword()
  defp put_agent(opts, spec, id) do
    case Spec.agent_for(spec, id) do
      nil -> opts
      agent -> Keyword.put_new(opts, :agent, agent)
    end
  end

  @spec put_store(keyword(), Spec.t()) :: keyword()
  defp put_store(opts, %Spec{store: nil}), do: opts

  defp put_store(opts, %Spec{store: store}), do: Keyword.put_new(opts, :idempotency_store, store)

  @spec report(map(), map(), term()) :: map()
  defp report(state, %{ref: %Ref{} = ref} = item, {:ok, receipt}) do
    publish(state, Event.new(:receipt, ref, %{receipt: receipt, outbound: item.outbound}))
    %{state | delivered: state.delivered + 1}
  end

  defp report(state, %{ref: %Ref{} = ref} = item, {:error, reason}) do
    publish(
      state,
      Event.new(:error, ref, %{stage: :deliver, reason: reason, outbound: item.outbound})
    )

    %{state | failed: state.failed + 1}
  end

  defp report(state, _item, {:ok, _receipt}), do: %{state | delivered: state.delivered + 1}
  defp report(state, _item, _result), do: %{state | failed: state.failed + 1}

  @spec publish(map(), Event.t()) :: :ok
  defp publish(state, event) do
    _stamped = Bus.publish(state.spec.bus, event)
    :ok
  end

  @spec await_drain(atom(), term(), integer()) :: :ok | {:error, :timeout}
  defp await_drain(gateway, id, deadline) do
    case info(gateway, id) do
      {:ok, %{queued: 0, inflight: 0}} ->
        :ok

      _pending ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(10)
          await_drain(gateway, id, deadline)
        end
    end
  end
end
