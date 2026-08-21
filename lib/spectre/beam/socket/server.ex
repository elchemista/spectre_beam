defmodule Spectre.Beam.Socket.Server do
  @moduledoc """
  Local control socket for a gateway.

  A Unix domain socket is the cheapest way to make the gateway reachable from
  outside the VM: a CLI, an editor plugin, a shell script, or a client in any
  language can drive it without joining the cluster or speaking distribution.

      {Spectre.Beam.Gateway,
       name: MyApp.Gateway,
       control: [socket: "/run/beam/gateway.sock"],
       channels: [...]}

  ## Security

  The socket is a full capability on the agent: anything that can write to it
  can send messages as the gateway. It is created with mode `0600` and owned
  by the running user, and a stale file left by a crashed node is removed at
  start. Do not place it in a world-writable directory, and do not widen its
  mode to share it — run a second socket instead.

  A `port:` option listens on loopback TCP instead, which is useful in a
  container where the socket file cannot be shared. It carries no
  authentication of its own, so bind it only to `127.0.0.1`.
  """

  use GenServer

  alias Spectre.Beam.Socket.Codec
  alias Spectre.Beam.Socket.Connection

  require Logger

  @registry Spectre.Beam.Registry
  @task_supervisor Spectre.Beam.TaskSupervisor
  @mode 0o600

  @type option ::
          {:gateway, atom()}
          | {:socket, Path.t()}
          | {:port, :inet.port_number()}
          | {:codec, Codec.t()}

  @doc false
  @spec child_spec({atom(), keyword()}) :: Supervisor.child_spec()
  def child_spec({gateway, opts}) do
    %{
      id: {__MODULE__, gateway},
      start: {__MODULE__, :start_link, [{gateway, opts}]},
      type: :worker
    }
  end

  @spec start_link({atom(), keyword()}) :: GenServer.on_start()
  def start_link({gateway, opts}) do
    GenServer.start_link(__MODULE__, {gateway, opts}, name: name(gateway))
  end

  @doc "Returns the via-tuple naming a gateway's control socket."
  @spec name(atom()) :: GenServer.name()
  def name(gateway), do: {:via, Registry, {@registry, {:socket, gateway}}}

  @doc "Returns the address the socket is listening on."
  @spec address(atom()) :: {:ok, term()} | {:error, :not_found}
  def address(gateway) do
    {:ok, GenServer.call(name(gateway), :address)}
  catch
    :exit, _reason -> {:error, :not_found}
  end

  @impl GenServer
  def init({gateway, opts}) do
    Process.flag(:trap_exit, true)
    codec = Keyword.get(opts, :codec, Codec.default())

    case listen(opts) do
      {:ok, socket, address} ->
        state = %{gateway: gateway, socket: socket, address: address, codec: codec}
        {:ok, state, {:continue, :accept}}

      {:error, reason} ->
        {:stop, {:beam_socket_listen_failed, reason}}
    end
  end

  @impl GenServer
  def handle_continue(:accept, state) do
    accept(state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:address, _from, state), do: {:reply, state.address, state}

  @impl GenServer
  def handle_info({:accepted, client}, state) do
    case Connection.start(state.gateway, client, state.codec) do
      {:ok, pid} ->
        :ok = :gen_tcp.controlling_process(client, pid)
        Process.send(pid, :ready, [])

      {:error, reason} ->
        Logger.warning("Beam socket connection refused: #{inspect(reason)}")
        :gen_tcp.close(client)
    end

    accept(state)
    {:noreply, state}
  end

  def handle_info({:accept_failed, reason}, state) do
    Logger.warning("Beam socket accept failed: #{inspect(reason)}")
    accept(state)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    :gen_tcp.close(state.socket)

    case state.address do
      {:local, path} -> File.rm(path)
      _other -> :ok
    end

    :ok
  end

  @spec listen(keyword()) :: {:ok, :gen_tcp.socket(), term()} | {:error, term()}
  defp listen(opts) do
    case {Keyword.get(opts, :socket), Keyword.get(opts, :port)} do
      {path, _port} when is_binary(path) -> listen_unix(path)
      {_path, port} when is_integer(port) -> listen_tcp(port, opts)
      _neither -> {:error, :beam_socket_address_required}
    end
  end

  @spec listen_unix(Path.t()) :: {:ok, :gen_tcp.socket(), term()} | {:error, term()}
  defp listen_unix(path) do
    with :ok <- prepare_path(path),
         {:ok, socket} <-
           :gen_tcp.listen(0, [
             {:ifaddr, {:local, path}},
             :binary,
             packet: 4,
             active: false,
             reuseaddr: true
           ]),
         :ok <- File.chmod(path, @mode) do
      {:ok, socket, {:local, path}}
    end
  end

  @spec listen_tcp(:inet.port_number(), keyword()) ::
          {:ok, :gen_tcp.socket(), term()} | {:error, term()}
  defp listen_tcp(port, opts) do
    address = Keyword.get(opts, :ip, {127, 0, 0, 1})

    with {:ok, socket} <-
           :gen_tcp.listen(port, [
             :binary,
             ip: address,
             packet: 4,
             active: false,
             reuseaddr: true
           ]),
         {:ok, bound} <- :inet.port(socket) do
      {:ok, socket, {:tcp, address, bound}}
    end
  end

  # A socket file left by a node that died is not a conflict to report: no
  # process is listening on it, and refusing to start would keep the gateway
  # down for a file nobody owns.
  @spec prepare_path(Path.t()) :: :ok | {:error, term()}
  defp prepare_path(path) do
    with :ok <- File.mkdir_p(Path.dirname(path)) do
      case File.stat(path) do
        {:ok, _stat} -> File.rm(path)
        {:error, :enoent} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec accept(map()) :: :ok
  defp accept(state) do
    server = self()
    socket = state.socket

    {:ok, _pid} =
      Task.Supervisor.start_child(@task_supervisor, fn ->
        case :gen_tcp.accept(socket) do
          {:ok, client} ->
            :ok = :gen_tcp.controlling_process(client, server)
            Process.send(server, {:accepted, client}, [])

          {:error, reason} when reason in [:closed, :einval] ->
            :ok

          {:error, reason} ->
            Process.send(server, {:accept_failed, reason}, [])
        end
      end)

    :ok
  end
end
