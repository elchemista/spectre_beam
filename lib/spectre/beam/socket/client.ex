defmodule Spectre.Beam.Socket.Client do
  @moduledoc """
  Client for a gateway's local control socket.

  It backs `mix beam.*` and any external tool that speaks the same frames. The
  connection is synchronous and owns no process, so a script can use it
  without a supervision tree.

      {:ok, conn} = Spectre.Beam.Socket.Client.connect("/run/beam/gateway.sock")
      {:ok, %{"text" => reply}} = Spectre.Beam.Socket.Client.request(conn, "ask",
        %{"ref" => "console:1", "text" => "ciao"})

  Event frames pushed by the server while a reply is pending are handed to the
  `:on_event` callback rather than dropped, so a subscribed client never loses
  what arrived mid-request.
  """

  alias Spectre.Beam.Socket.Codec

  @type conn :: %{socket: :gen_tcp.socket(), codec: Codec.t()}

  @default_timeout :timer.seconds(60)

  @doc """
  Connects to a control socket.

  Accepts a path, `{ip, port}`, or options with `:socket` / `:port` and an
  optional `:codec`.
  """
  @spec connect(Path.t() | {tuple(), :inet.port_number()} | keyword(), keyword()) ::
          {:ok, conn()} | {:error, term()}
  def connect(target, opts \\ [])

  def connect(path, opts) when is_binary(path) do
    connect_to({:local, path}, 0, opts)
  end

  def connect({address, port}, opts) when is_integer(port) do
    connect_to(address, port, opts)
  end

  def connect(opts, _extra) when is_list(opts) do
    case {Keyword.get(opts, :socket), Keyword.get(opts, :port)} do
      {path, _port} when is_binary(path) ->
        connect(path, opts)

      {_path, port} when is_integer(port) ->
        connect({Keyword.get(opts, :ip, ~c"127.0.0.1"), port}, opts)

      _neither ->
        {:error, :beam_socket_address_required}
    end
  end

  @doc "Closes the connection."
  @spec close(conn()) :: :ok
  def close(conn), do: :gen_tcp.close(conn.socket)

  @doc """
  Sends one request and waits for its reply.

  Options: `:timeout`, `:on_event` — a one-argument function called with every
  event frame that arrives while waiting.
  """
  @spec request(conn(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def request(conn, op, params \\ %{}, opts \\ []) do
    id = System.unique_integer([:positive, :monotonic])
    payload = params |> Map.put("op", op) |> Map.put("id", id)

    with {:ok, deadline} <- deadline(opts),
         {:ok, frame} <- Codec.encode(conn.codec, payload),
         :ok <- :gen_tcp.send(conn.socket, frame) do
      await(conn, id, deadline, Keyword.get(opts, :on_event))
    end
  end

  @doc """
  Subscribes to a conversation and calls `handler` for every event received.

  Returns when `handler` returns `:halt` or the read times out.
  """
  @spec follow(conn(), String.t(), (map() -> :cont | :halt), keyword()) :: :ok | {:error, term()}
  def follow(conn, ref, handler, opts \\ []) do
    with {:ok, _data} <- request(conn, "subscribe", %{"ref" => ref}, opts) do
      consume(conn, handler, Keyword.get(opts, :timeout, :infinity))
    end
  end

  @spec connect_to(term(), :inet.port_number(), keyword()) :: {:ok, conn()} | {:error, term()}
  defp connect_to(address, port, opts) do
    codec = Keyword.get(opts, :codec, Codec.default())
    timeout = Keyword.get(opts, :connect_timeout, 5_000)

    case :gen_tcp.connect(address, port, [:binary, packet: 4, active: false], timeout) do
      {:ok, socket} -> {:ok, %{socket: socket, codec: codec}}
      {:error, reason} -> {:error, {:beam_socket_connect_failed, reason}}
    end
  end

  @spec await(conn(), integer(), integer() | :infinity, (map() -> any()) | nil) ::
          {:ok, map()} | {:error, term()}
  defp await(conn, id, deadline, on_event) do
    timeout = remaining(deadline)

    with {:ok, frame} <- recv(conn, timeout),
         {:ok, payload} <- Codec.decode(conn.codec, frame) do
      route(payload, conn, id, deadline, on_event)
    end
  end

  @spec route(map(), conn(), integer(), integer() | :infinity, (map() -> any()) | nil) ::
          {:ok, map()} | {:error, term()}
  defp route(%{"event" => event}, conn, id, deadline, on_event) do
    if on_event, do: on_event.(event)
    await(conn, id, deadline, on_event)
  end

  defp route(payload, conn, id, deadline, on_event) do
    if Map.get(payload, "id") == id,
      do: unwrap(payload),
      else: await(conn, id, deadline, on_event)
  end

  @spec consume(conn(), (map() -> :cont | :halt), timeout()) :: :ok | {:error, term()}
  defp consume(conn, handler, timeout) do
    with {:ok, frame} <- recv(conn, timeout),
         {:ok, payload} <- Codec.decode(conn.codec, frame) do
      deliver(payload, conn, handler, timeout)
    end
  end

  @spec deliver(map(), conn(), (map() -> :cont | :halt), timeout()) :: :ok | {:error, term()}
  defp deliver(%{"event" => event}, conn, handler, timeout) do
    if handler.(event) == :halt, do: :ok, else: consume(conn, handler, timeout)
  end

  defp deliver(_reply, conn, handler, timeout), do: consume(conn, handler, timeout)

  @spec recv(conn(), timeout()) :: {:ok, binary()} | {:error, term()}
  defp recv(conn, timeout) do
    case :gen_tcp.recv(conn.socket, 0, timeout) do
      {:ok, frame} -> {:ok, frame}
      {:error, reason} -> {:error, {:beam_socket_recv_failed, reason}}
    end
  end

  @spec unwrap(map()) :: {:ok, map()} | {:error, term()}
  defp unwrap(%{"ok" => true} = payload), do: {:ok, Map.get(payload, "data", %{})}
  defp unwrap(%{"ok" => false} = payload), do: {:error, Map.get(payload, "error")}
  defp unwrap(payload), do: {:error, {:invalid_beam_socket_reply, payload}}

  @spec deadline(keyword()) :: {:ok, integer() | :infinity} | {:error, term()}
  defp deadline(opts) do
    case Keyword.get(opts, :timeout, @default_timeout) do
      :infinity ->
        {:ok, :infinity}

      timeout when is_integer(timeout) and timeout >= 0 ->
        {:ok, System.monotonic_time(:millisecond) + timeout}

      timeout ->
        {:error, {:invalid_beam_timeout, timeout}}
    end
  end

  @spec remaining(integer() | :infinity) :: timeout()
  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
end
