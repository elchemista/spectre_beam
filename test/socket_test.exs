defmodule Spectre.Beam.SocketTest.Model do
  @moduledoc false

  def complete(_prompt, _opts), do: {:ok, "unused"}
end

defmodule Spectre.Beam.SocketTest.Agent do
  @moduledoc false

  use Spectre.Agent, prompt_root: "test/fixtures/prompts"

  model(Spectre.Beam.SocketTest.Model)
  use Spectre.Beam

  beaming do
    channel(:console, type: :console, adapter: Spectre.Beam.Adapters.Local)
  end

  flow :console_flow do
    on :question, regex: ~r/question/i do
      reply(:generic_reply)
    end
  end
end

defmodule Spectre.Beam.SocketTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Adapters
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Socket.Client
  alias Spectre.Beam.Socket.Codec
  alias Spectre.Beam.Socket.Protocol
  alias Spectre.Beam.Socket.Server

  setup do
    name = :"beam_socket_#{System.unique_integer([:positive])}"
    path = Path.join(System.tmp_dir!(), "#{name}.sock")

    start_supervised!(
      {Gateway,
       name: name,
       agent: Spectre.Beam.SocketTest.Agent,
       control: [socket: path],
       channels: [console: [type: :console, adapter: Adapters.Local]]}
    )

    on_exit(fn -> File.rm(path) end)

    {:ok, conn} = Client.connect(path)
    on_exit(fn -> Client.close(conn) end)

    %{gateway: name, path: path, conn: conn}
  end

  test "creates the socket with owner-only permissions", %{path: path, gateway: gateway} do
    assert {:ok, {:local, ^path}} = Server.address(gateway)
    assert {:ok, %File.Stat{mode: mode}} = File.stat(path)
    assert Bitwise.band(mode, 0o777) == 0o600
  end

  test "answers hello with the gateway identity", %{conn: conn, gateway: gateway} do
    assert {:ok, data} = Client.request(conn, "hello")
    assert data["gateway"] == to_string(gateway)
    assert data["version"] == Spectre.Beam.version()
    assert "reply" in data["events"]
  end

  test "lists endpoints and conversations", %{conn: conn} do
    assert {:ok, %{"endpoints" => [endpoint]}} = Client.request(conn, "endpoints")
    assert endpoint["endpoint"] == "console"

    assert {:ok, %{"ref" => "console:cli"}} =
             Client.request(conn, "open", %{"ref" => "console:cli"})

    assert {:ok, %{"conversations" => conversations}} = Client.request(conn, "conversations")
    assert "console:cli" in conversations
  end

  test "asks the agent and returns its reply", %{conn: conn} do
    assert {:ok, data} =
             Client.request(conn, "ask", %{"ref" => "console:ask", "text" => "question"},
               timeout: 10_000
             )

    assert data["ref"] == "console:ask"
    assert is_binary(data["text"])
  end

  test "pushes subscribed events while a request is in flight", %{conn: conn} do
    assert {:ok, _subscribed} = Client.request(conn, "subscribe", %{"ref" => "console:sub"})

    owner = self()

    assert {:ok, _sent} =
             Client.request(conn, "send", %{"ref" => "console:sub", "text" => "question"})

    assert :ok =
             Client.follow(
               conn,
               "console:sub",
               fn event ->
                 send(owner, {:socket_event, event})
                 if event["type"] == "reply", do: :halt, else: :cont
               end,
               timeout: :infinity
             )

    assert_receive {:socket_event, %{"type" => "reply", "payload" => %{"text" => text}}}, 1_000
    assert is_binary(text)
  end

  test "returns retained history with sequence numbers", %{conn: conn} do
    {:ok, _reply} =
      Client.request(conn, "ask", %{"ref" => "console:hist", "text" => "question"},
        timeout: 10_000
      )

    assert {:ok, %{"events" => events}} =
             Client.request(conn, "history", %{"ref" => "console:hist"})

    assert length(events) > 1
    sequences = Enum.map(events, & &1["seq"])
    assert sequences == Enum.sort(sequences)
  end

  test "reports an unknown operation without closing the connection", %{conn: conn} do
    assert {:error, {:invalid_beam_timeout, :forever}} =
             Client.request(conn, "hello", %{}, timeout: :forever)

    assert {:error, error} = Client.request(conn, "nope")
    assert error =~ "unknown_beam_socket_op"

    assert {:ok, _hello} = Client.request(conn, "hello")
  end

  test "rejects an invalid ask timeout without closing the connection", %{conn: conn} do
    assert {:error, error} =
             Client.request(conn, "ask", %{
               "ref" => "console:bad-timeout",
               "text" => "question",
               "timeout" => "forever"
             })

    assert error =~ "invalid_beam_timeout"
    assert {:ok, _hello} = Client.request(conn, "hello")
  end

  test "runs the doctor over the socket", %{conn: conn} do
    assert {:ok, %{"checks" => checks, "verdict" => verdict}} = Client.request(conn, "doctor")
    assert is_list(checks)
    assert verdict in ["ok", "warn", "error"]
  end

  test "supports the ETF control path for direct Elixir clients" do
    gateway = :"beam_etf_#{System.unique_integer([:positive])}"
    path = Path.join(System.tmp_dir!(), "#{gateway}.sock")

    start_supervised!(
      {Gateway,
       name: gateway,
       control: [socket: path, codec: :etf],
       channels: [console: [type: :console, adapter: Adapters.Local]]}
    )

    on_exit(fn -> File.rm(path) end)
    assert {:ok, conn} = Client.connect(path, codec: :etf)
    on_exit(fn -> Client.close(conn) end)

    assert {:ok, %{"gateway" => encoded_gateway}} = Client.request(conn, "hello")
    assert encoded_gateway == to_string(gateway)
  end

  test "supports loopback TCP and option-based clients" do
    gateway = :"beam_tcp_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Gateway,
       name: gateway,
       control: [port: 0, ip: {127, 0, 0, 1}],
       channels: [console: [type: :console, adapter: Adapters.Local]]}
    )

    assert {:ok, {:tcp, {127, 0, 0, 1}, port}} = Server.address(gateway)
    assert {:ok, conn} = Client.connect(port: port, ip: {127, 0, 0, 1})
    assert {:ok, %{"gateway" => encoded}} = Client.request(conn, "hello")
    assert encoded == to_string(gateway)
    assert :ok = Client.close(conn)

    assert Client.connect([]) == {:error, :beam_socket_address_required}
    assert {:error, {:beam_socket_connect_failed, _}} = Client.connect({{127, 0, 0, 1}, 1})
  end

  test "codec rejects malformed and non-map frames" do
    assert {:error, {:invalid_beam_socket_frame, [:not, :a, :map]}} =
             Codec.decode(:etf, :erlang.term_to_binary([:not, :a, :map]))

    assert {:error, :invalid_beam_socket_frame} = Codec.decode(:etf, <<1, 2, 3>>)

    if Codec.json_available?() do
      assert {:error, {:invalid_beam_socket_frame, [1, 2]}} = Codec.decode(:json, "[1,2]")
      assert {:error, {:beam_socket_decode_failed, _}} = Codec.decode(:json, "{")
    end
  end

  test "connection returns a framed error for malformed wire input", %{path: path} do
    assert {:ok, socket} =
             :gen_tcp.connect({:local, path}, 0, [:binary, packet: 4, active: false])

    assert :ok = :gen_tcp.send(socket, "{")
    assert {:ok, frame} = :gen_tcp.recv(socket, 0, 1_000)
    assert {:ok, %{"ok" => false, "error" => error}} = Codec.decode(Codec.default(), frame)
    assert error =~ "beam_socket_decode_failed"
    assert :ok = :gen_tcp.close(socket)
  end

  test "protocol supports conversation administration and validates inputs", %{gateway: gateway} do
    state = Protocol.init(gateway)

    {reply, ^state} = Protocol.handle(%{"id" => 1, "op" => nil}, state)
    refute reply["ok"]
    assert reply["error"] =~ "invalid_beam_socket_op"

    for request <- [
          %{"op" => "open"},
          %{"op" => "open", "ref" => 12},
          %{"op" => "send", "ref" => "console:admin"},
          %{"op" => "send", "ref" => "missing:admin", "text" => "hello"}
        ] do
      {reply, ^state} = Protocol.handle(request, state)
      refute reply["ok"]
    end

    {subscribed, state} =
      Protocol.handle(%{"op" => "subscribe", "ref" => "console:admin"}, state)

    assert subscribed["ok"]
    {:ok, ref} = Gateway.open(gateway, "console:admin")
    event = Spectre.Beam.Event.new(:reply, ref, %{text: "hello"})
    assert Protocol.subscribed?(state, event)
    assert %{"event" => %{"type" => "reply"}} = Protocol.event_frame(event)

    {status, state} = Protocol.handle(%{"op" => "status", "ref" => "console:admin"}, state)
    assert status["data"]["status"] =~ "idle"

    {history, state} =
      Protocol.handle(
        %{"op" => "history", "ref" => "console:admin", "after" => 0, "limit" => 2},
        state
      )

    assert history["ok"]

    {cancelled, state} = Protocol.handle(%{"op" => "cancel", "ref" => "console:admin"}, state)
    assert cancelled["data"]["cancelled"]

    {unsubscribed, state} =
      Protocol.handle(%{"op" => "unsubscribe", "ref" => "console:admin"}, state)

    assert unsubscribed["data"]["subscribed"] == false
    refute Protocol.subscribed?(state, event)

    {closed, _state} = Protocol.handle(%{"op" => "close", "ref" => "console:admin"}, state)
    assert closed["data"]["closed"]
  end
end
