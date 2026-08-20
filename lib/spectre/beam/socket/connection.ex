defmodule Spectre.Beam.Socket.Connection do
  @moduledoc """
  One accepted control-socket connection.

  The process both answers requests and forwards the bus events the client
  subscribed to, which is why it owns the socket in `active: :once` mode: a
  blocking read would make it deaf to the events it exists to deliver.
  """

  use GenServer, restart: :temporary

  alias Spectre.Beam.Event
  alias Spectre.Beam.Socket.Codec
  alias Spectre.Beam.Socket.Protocol

  require Logger

  @doc false
  @spec start(atom(), :gen_tcp.socket(), Codec.t()) :: GenServer.on_start()
  def start(gateway, socket, codec) do
    GenServer.start(__MODULE__, {gateway, socket, codec})
  end

  @impl GenServer
  def init({gateway, socket, codec}) do
    {:ok, %{socket: socket, codec: codec, protocol: Protocol.init(gateway)}}
  end

  @impl GenServer
  def handle_info(:ready, state) do
    :ok = :inet.setopts(state.socket, active: :once)
    {:noreply, state}
  end

  def handle_info({:tcp, socket, frame}, %{socket: socket} = state) do
    state = handle_frame(frame, state)
    :ok = :inet.setopts(socket, active: :once)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state), do: {:stop, :normal, state}

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    Logger.debug("Beam socket connection error: #{inspect(reason)}")
    {:stop, :normal, state}
  end

  def handle_info(%Event{} = event, state) do
    if Protocol.subscribed?(state.protocol, event),
      do: write(state, Protocol.event_frame(event))

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    :gen_tcp.close(state.socket)
    :ok
  end

  @spec handle_frame(binary(), map()) :: map()
  defp handle_frame(frame, state) do
    case Codec.decode(state.codec, frame) do
      {:ok, request} ->
        {reply, protocol} = Protocol.handle(request, state.protocol)
        state = %{state | protocol: protocol}
        write(state, reply)
        state

      {:error, reason} ->
        write(state, %{"id" => nil, "ok" => false, "error" => inspect(reason)})
        state
    end
  end

  @spec write(map(), map()) :: :ok
  defp write(state, payload) do
    case Codec.encode(state.codec, payload) do
      {:ok, frame} ->
        _sent = :gen_tcp.send(state.socket, frame)
        :ok

      {:error, reason} ->
        Logger.warning("Beam socket encode failed: #{inspect(reason)}")
        :ok
    end
  end
end
