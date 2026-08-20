defmodule Spectre.Beam.Endpoint.Server do
  @moduledoc """
  Owner of one mounted channel at runtime.

  Today a caller passes `adapter_opts: [client: session]` on every call, which
  makes the provider connection the host's problem. Under a gateway the
  endpoint process owns it: it resolves the client once, runs the subscription
  lifecycle on start and stop, drives the configured ingress, and answers with
  the adapter options every delivery needs.

  It never performs provider I/O inside a call. Polling runs in a supervised
  task, so `adapter_opts/2` and `status/2` stay immediate even while a slow
  provider is being read.
  """

  use GenServer

  alias Spectre.Beam.Gateway.Spec
  alias Spectre.Beam.Runtime
  alias Spectre.Beam.Telemetry

  require Logger

  @registry Spectre.Beam.Registry
  @task_supervisor Spectre.Beam.TaskSupervisor
  @default_poll_interval_ms 1_000

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

  @doc "Returns the via-tuple naming an endpoint server."
  @spec name(atom(), term()) :: GenServer.name()
  def name(gateway, id), do: {:via, Registry, {@registry, {:endpoint, gateway, id}}}

  @doc """
  Returns the runtime adapter options for one endpoint, client included.

  Callers pass the result as `adapter_opts:` so the resolved provider client
  reaches the adapter without ever leaving the gateway.
  """
  @spec adapter_opts(atom(), term()) :: keyword()
  def adapter_opts(gateway, id) do
    GenServer.call(name(gateway, id), :adapter_opts)
  catch
    :exit, _reason -> []
  end

  @doc "Returns the observable state of one endpoint."
  @spec status(atom(), term()) :: {:ok, map()} | {:error, :not_found}
  def status(gateway, id) do
    {:ok, GenServer.call(name(gateway, id), :status)}
  catch
    :exit, _reason -> {:error, :not_found}
  end

  @doc "Records that an event was accepted, for reporting only."
  @spec record_event(atom(), term()) :: :ok
  def record_event(gateway, id) do
    GenServer.cast(name(gateway, id), :record_event)
  catch
    :exit, _reason -> :ok
  end

  @impl GenServer
  def init({spec, id}) do
    Process.flag(:trap_exit, true)

    with {:ok, endpoint} <- Spec.endpoint(spec, id),
         {:ok, channel} <- Spec.channel(spec, id),
         {:ok, client} <- resolve_client(channel.client) do
      state = %{
        gateway: spec.name,
        id: id,
        spec: spec,
        endpoint: endpoint,
        channel: channel,
        adapter_opts: adapter_options(spec.name, id, client, channel),
        subscribed?: false,
        poll_task: nil,
        events: 0,
        last_event_at: nil,
        status: :up
      }

      {:ok, state, {:continue, :start_ingress}}
    else
      {:error, reason} -> {:stop, {:invalid_beam_endpoint, id, reason}}
    end
  end

  @impl GenServer
  def handle_continue(:start_ingress, state) do
    {:noreply, start_ingress(state)}
  end

  @impl GenServer
  def handle_call(:adapter_opts, _from, state), do: {:reply, state.adapter_opts, state}

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       gateway: state.gateway,
       endpoint: state.id,
       type: state.endpoint.type,
       adapter: state.endpoint.adapter,
       ingress: ingress_kind(state.channel.ingress),
       subscribed?: state.subscribed?,
       events: state.events,
       last_event_at: state.last_event_at,
       status: state.status
     }, state}
  end

  @impl GenServer
  def handle_cast(:record_event, state) do
    {:noreply, %{state | events: state.events + 1, last_event_at: DateTime.utc_now()}}
  end

  @impl GenServer
  def handle_info(:poll, %{poll_task: nil} = state) do
    {:noreply, dispatch_poll(state)}
  end

  # A poll still running when the next tick fires is not doubled up; the timer
  # is simply rearmed after the running fetch reports.
  def handle_info(:poll, state), do: {:noreply, state}

  def handle_info({ref, result}, %{poll_task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, state |> handle_poll_result(result) |> schedule_poll()}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{poll_task: %Task{ref: ref}} = state) do
    Logger.warning("Beam ingress poll failed on #{inspect(state.id)}: #{inspect(reason)}")
    {:noreply, state |> Map.put(:poll_task, nil) |> schedule_poll()}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    if state.subscribed?, do: call_lifecycle(state, :unsubscribe)
    :ok
  end

  @spec start_ingress(map()) :: map()
  defp start_ingress(%{channel: %{ingress: :none}} = state), do: state

  defp start_ingress(%{channel: %{ingress: :subscribe}} = state), do: do_subscribe(state)

  defp start_ingress(%{channel: %{ingress: {:poll, opts}}} = state) do
    state
    |> then(fn state ->
      if Keyword.get(opts, :subscribe, false), do: do_subscribe(state), else: state
    end)
    |> schedule_poll(0)
  end

  @spec do_subscribe(map()) :: map()
  defp do_subscribe(state) do
    case call_lifecycle(state, :subscribe) do
      :ok ->
        %{state | subscribed?: true}

      {:error, reason} ->
        Logger.warning("Beam subscribe failed on #{inspect(state.id)}: #{inspect(reason)}")
        %{state | status: {:degraded, reason}}
    end
  end

  @spec schedule_poll(map(), non_neg_integer() | nil) :: map()
  defp schedule_poll(state, delay \\ nil)

  defp schedule_poll(%{channel: %{ingress: {:poll, opts}}} = state, delay) do
    interval = delay || Keyword.get(opts, :interval_ms, @default_poll_interval_ms)
    _timer = Process.send_after(self(), :poll, interval)
    %{state | poll_task: nil}
  end

  defp schedule_poll(state, _delay), do: %{state | poll_task: nil}

  @spec dispatch_poll(map()) :: map()
  defp dispatch_poll(%{channel: %{ingress: {:poll, opts}}} = state) do
    fetch = Keyword.fetch!(opts, :fetch)
    adapter_opts = state.adapter_opts

    task =
      Task.Supervisor.async_nolink(@task_supervisor, fn ->
        Telemetry.span(:ingress, %{gateway: state.gateway, endpoint: state.id}, fn ->
          call_fetch(fetch, adapter_opts)
        end)
      end)

    %{state | poll_task: task}
  end

  defp dispatch_poll(state), do: state

  @spec call_fetch(term(), keyword()) :: term()
  defp call_fetch(fetch, adapter_opts) when is_function(fetch, 1), do: fetch.(adapter_opts)

  defp call_fetch({module, function, args}, adapter_opts),
    do: apply(module, function, args ++ [adapter_opts])

  @spec handle_poll_result(map(), term()) :: map()
  defp handle_poll_result(state, {:ok, events}) when is_list(events) do
    Enum.each(events, &Spectre.Beam.Gateway.ingest(state.gateway, state.id, &1))
    %{state | poll_task: nil}
  end

  defp handle_poll_result(state, :ok), do: %{state | poll_task: nil}
  defp handle_poll_result(state, :ignore), do: %{state | poll_task: nil}

  defp handle_poll_result(state, {:error, reason}) do
    Logger.warning("Beam ingress poll error on #{inspect(state.id)}: #{inspect(reason)}")
    %{state | poll_task: nil}
  end

  defp handle_poll_result(state, other) do
    Logger.warning("Beam ingress poll returned #{inspect(other)} on #{inspect(state.id)}")
    %{state | poll_task: nil}
  end

  @spec call_lifecycle(map(), :subscribe | :unsubscribe) :: :ok | {:error, term()}
  defp call_lifecycle(state, callback) do
    apply(Runtime, callback, [state.spec.beam, state.id, [adapter_opts: state.adapter_opts]])
  end

  @spec adapter_options(atom(), term(), term(), Spec.channel()) :: keyword()
  defp adapter_options(gateway, id, client, channel) do
    [gateway: gateway, endpoint: id]
    |> put_option(:client, client)
    |> put_option(:notify, channel.notify)
  end

  @spec put_option(keyword(), atom(), term()) :: keyword()
  defp put_option(opts, _key, nil), do: opts
  defp put_option(opts, key, value), do: Keyword.put(opts, key, value)

  @spec resolve_client(term()) :: {:ok, term()} | {:error, term()}
  defp resolve_client(nil), do: {:ok, nil}

  defp resolve_client(fun) when is_function(fun, 0), do: normalize_client(fun.())

  defp resolve_client({module, function, args})
       when is_atom(module) and is_atom(function) and is_list(args) do
    normalize_client(apply(module, function, args))
  rescue
    exception -> {:error, {:beam_client_resolution_failed, Exception.message(exception)}}
  end

  defp resolve_client(client), do: {:ok, client}

  @spec normalize_client(term()) :: {:ok, term()} | {:error, term()}
  defp normalize_client({:ok, client}), do: {:ok, client}
  defp normalize_client({:error, _reason} = error), do: error
  defp normalize_client(client), do: {:ok, client}

  @spec ingress_kind(term()) :: atom()
  defp ingress_kind(:none), do: :none
  defp ingress_kind(:subscribe), do: :subscribe
  defp ingress_kind({:poll, _opts}), do: :poll
end
