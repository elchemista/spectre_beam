defmodule Spectre.Beam.CLI do
  @moduledoc """
  Command implementations shared by the `mix beam.*` tasks.

  Every command runs in one of two modes. In-VM it calls the gateway directly,
  which is what `mix beam.chat` inside a project does. With `--socket PATH` it
  speaks the control protocol instead, so the same commands drive a gateway in
  a running release from an ordinary terminal — no IEx, no distribution, no
  code loaded from the target node.

  Keeping both behind one module is what will let an escript ship the same
  commands without duplicating them.
  """

  alias Spectre.Beam.Console
  alias Spectre.Beam.Doctor
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Ref
  alias Spectre.Beam.Socket.Client

  @type mode :: {:vm, atom()} | {:socket, Client.conn()}

  @switches [
    gateway: :string,
    endpoint: :string,
    socket: :string,
    port: :integer,
    timeout: :integer,
    limit: :integer,
    sender: :string
  ]

  @doc "Returns the option parser definition shared by every task."
  @spec switches() :: keyword()
  def switches, do: @switches

  @doc "Parses task arguments into options and positional values."
  @spec parse([String.t()]) :: {keyword(), [String.t()]}
  def parse(args) do
    {opts, rest, _invalid} = OptionParser.parse(args, strict: @switches)
    {opts, rest}
  end

  @doc """
  Opens the connection a command should use.

  A `:socket` or `:port` option selects the control socket; otherwise the
  command talks to a gateway in this VM.
  """
  @spec connect(keyword()) :: {:ok, mode()} | {:error, term()}
  def connect(opts) do
    if opts[:socket] || opts[:port] do
      with {:ok, conn} <- Client.connect(opts), do: {:ok, {:socket, conn}}
    else
      with {:ok, gateway} <- gateway(opts), do: {:ok, {:vm, gateway}}
    end
  end

  @doc "Closes a connection opened by `connect/1`."
  @spec disconnect(mode()) :: :ok
  def disconnect({:socket, conn}), do: Client.close(conn)
  def disconnect({:vm, _gateway}), do: :ok

  @doc "Prints one line per mounted endpoint."
  @spec status(mode()) :: :ok
  def status({:vm, gateway}) do
    IO.puts("gateway #{gateway}")

    Enum.each(Gateway.endpoints(gateway), fn endpoint ->
      IO.puts(
        "  " <>
          String.pad_trailing(to_string(endpoint.endpoint), 16) <>
          String.pad_trailing(to_string(Map.get(endpoint, :ingress, :none)), 12) <>
          String.pad_trailing(inspect(endpoint.status), 14) <>
          "#{Map.get(endpoint, :events, 0)} events"
      )
    end)

    IO.puts("\nconversations")
    Enum.each(Gateway.conversations(gateway), &IO.puts("  " <> Ref.slug(&1)))
  end

  def status({:socket, conn}) do
    with {:ok, hello} <- Client.request(conn, "hello"),
         {:ok, %{"endpoints" => endpoints}} <- Client.request(conn, "endpoints"),
         {:ok, %{"conversations" => conversations}} <- Client.request(conn, "conversations") do
      IO.puts("gateway #{hello["gateway"]} (beam #{hello["version"]})")

      Enum.each(endpoints, fn endpoint ->
        IO.puts(
          "  " <>
            String.pad_trailing(endpoint["endpoint"], 16) <>
            String.pad_trailing(endpoint["ingress"], 12) <>
            String.pad_trailing(endpoint["status"], 14) <>
            "#{endpoint["events"]} events"
        )
      end)

      IO.puts("\nconversations")
      Enum.each(conversations, &IO.puts("  " <> &1))
    else
      {:error, reason} -> abort(reason)
    end
  end

  @doc "Prints a health report."
  @spec doctor(mode()) :: :ok
  def doctor({:vm, gateway}), do: Doctor.report(gateway)

  def doctor({:socket, conn}) do
    case Client.request(conn, "doctor") do
      {:ok, %{"checks" => checks, "verdict" => verdict}} ->
        Enum.each(checks, fn check ->
          IO.puts([
            String.pad_trailing(check["status"], 6),
            String.pad_trailing(check["scope"], 22),
            String.pad_trailing(check["check"], 20),
            check["detail"]
          ])
        end)

        IO.puts("\n#{verdict}  #{length(checks)} checks")

      {:error, reason} ->
        abort(reason)
    end
  end

  @doc "Sends one message and prints the reply."
  @spec ask(mode(), String.t(), String.t(), keyword()) :: :ok
  def ask({:vm, gateway}, target, text, opts) do
    with {:ok, ref} <- Gateway.open(gateway, target),
         {:ok, reply} <- Spectre.Beam.Chat.ask(ref, text, ask_opts(opts)) do
      IO.puts(reply)
    else
      {:error, reason} -> abort(reason)
    end
  end

  def ask({:socket, conn}, target, text, opts) do
    params = %{"ref" => target, "text" => text} |> maybe_put("sender", opts[:sender])

    case Client.request(conn, "ask", params, timeout: timeout(opts)) do
      {:ok, %{"text" => reply}} -> IO.puts(reply)
      {:error, reason} -> abort(reason)
    end
  end

  @doc "Delivers one message without producing a turn."
  @spec push(mode(), String.t(), String.t(), keyword()) :: :ok
  def push({:vm, gateway}, target, text, _opts) do
    case Gateway.push(gateway, target, text) do
      {:ok, ref} -> IO.puts("queued on #{Ref.slug(ref)}")
      {:error, reason} -> abort(reason)
    end
  end

  def push({:socket, conn}, target, text, opts) do
    case Client.request(conn, "push", %{"ref" => target, "text" => text}, timeout: timeout(opts)) do
      {:ok, %{"ref" => ref}} -> IO.puts("queued on #{ref}")
      {:error, reason} -> abort(reason)
    end
  end

  @doc "Follows a conversation until interrupted."
  @spec tail(mode(), String.t(), keyword()) :: :ok
  def tail({:vm, gateway}, target, opts) do
    case Gateway.open(gateway, target) do
      {:ok, ref} -> Console.tail(ref, Keyword.take(opts, [:limit]))
      {:error, reason} -> abort(reason)
    end
  end

  def tail({:socket, conn}, target, _opts) do
    IO.puts("— following #{target}, interrupt to stop —")

    Client.follow(
      conn,
      target,
      fn event ->
        print_event(event)
        :cont
      end,
      timeout: :infinity
    )

    :ok
  end

  @doc "Starts an interactive conversation."
  @spec chat(mode(), String.t() | nil, keyword()) :: :ok
  def chat({:vm, gateway}, target, opts) do
    console_opts =
      opts
      |> Keyword.take([:endpoint, :timeout, :sender])
      |> Keyword.put(:gateway, gateway)
      |> normalize_endpoint()

    Console.chat(target, console_opts)
  end

  def chat({:socket, conn}, target, opts) do
    ref = target || "console:" <> Base.url_encode64(:crypto.strong_rand_bytes(4), padding: false)

    case Client.request(conn, "open", %{"ref" => ref}) do
      {:ok, %{"ref" => opened}} ->
        IO.puts("beam · #{opened} · over control socket")
        IO.puts("/exit to leave\n")
        socket_loop(conn, opened, opts)

      {:error, reason} ->
        abort(reason)
    end
  end

  @spec socket_loop(Client.conn(), String.t(), keyword()) :: :ok
  defp socket_loop(conn, ref, opts) do
    case IO.gets("you › ") do
      :eof -> :ok
      {:error, _reason} -> :ok
      line -> socket_line(String.trim(line), conn, ref, opts)
    end
  end

  @spec socket_line(String.t(), Client.conn(), String.t(), keyword()) :: :ok
  defp socket_line("", conn, ref, opts), do: socket_loop(conn, ref, opts)
  defp socket_line("/exit", _conn, _ref, _opts), do: :ok
  defp socket_line("/quit", _conn, _ref, _opts), do: :ok

  defp socket_line(text, conn, ref, opts) do
    case Client.request(conn, "ask", %{"ref" => ref, "text" => text}, timeout: timeout(opts)) do
      {:ok, %{"text" => reply}} -> IO.puts("bot › #{reply}")
      {:error, reason} -> IO.puts(:stderr, "  ! #{inspect(reason)}")
    end

    socket_loop(conn, ref, opts)
  end

  @spec gateway(keyword()) :: {:ok, atom()} | {:error, term()}
  defp gateway(opts) do
    case opts[:gateway] do
      name when is_binary(name) ->
        {:ok, String.to_atom(name)}

      nil ->
        case Gateway.list() do
          [only] -> {:ok, only}
          [] -> {:error, :no_beam_gateway_running}
          many -> {:error, {:ambiguous_beam_gateway, many}}
        end
    end
  end

  @spec normalize_endpoint(keyword()) :: keyword()
  defp normalize_endpoint(opts) do
    case Keyword.get(opts, :endpoint) do
      id when is_binary(id) -> Keyword.put(opts, :endpoint, String.to_atom(id))
      _other -> opts
    end
  end

  @spec ask_opts(keyword()) :: keyword()
  defp ask_opts(opts) do
    []
    |> maybe_put_kw(:timeout, opts[:timeout])
    |> maybe_put_kw(:sender, opts[:sender])
  end

  @spec timeout(keyword()) :: pos_integer()
  defp timeout(opts), do: opts[:timeout] || :timer.seconds(60)

  @spec maybe_put(map(), String.t(), term()) :: map()
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  @spec maybe_put_kw(keyword(), atom(), term()) :: keyword()
  defp maybe_put_kw(opts, _key, nil), do: opts
  defp maybe_put_kw(opts, key, value), do: Keyword.put(opts, key, value)

  @spec print_event(map()) :: :ok
  defp print_event(%{"type" => "reply", "payload" => %{"text" => text}}),
    do: IO.puts("bot › #{text}")

  defp print_event(%{"type" => "inbound", "payload" => payload}),
    do: IO.puts("#{Map.get(payload, "sender", "?")} › #{Map.get(payload, "text")}")

  defp print_event(%{"type" => "error", "payload" => payload}),
    do: IO.puts(:stderr, "  ! #{inspect(payload)}")

  defp print_event(_event), do: :ok

  @spec abort(term()) :: no_return()
  defp abort(reason) do
    IO.puts(:stderr, "beam: #{inspect(reason)}")
    exit({:shutdown, 1})
  end
end
