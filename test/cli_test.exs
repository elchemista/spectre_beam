defmodule Spectre.Beam.CLITest.Model do
  @moduledoc false
  def complete(_prompt, _opts), do: {:ok, "unused"}
end

defmodule Spectre.Beam.CLITest.Agent do
  @moduledoc false

  use Spectre.Agent, prompt_root: "test/fixtures/prompts"

  model(Spectre.Beam.CLITest.Model)
  use Spectre.Beam

  flow :console_flow do
    on :question, regex: ~r/question/i do
      reply(:generic_reply)
    end
  end
end

defmodule Spectre.Beam.CLITest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Spectre.Beam.Adapters
  alias Spectre.Beam.CLI
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Socket.Client

  setup do
    name = :beam_cli_test

    path =
      Path.join(System.tmp_dir!(), "beam_cli_test_#{System.unique_integer([:positive])}.sock")

    start_supervised!(
      {Gateway,
       name: name,
       agent: Spectre.Beam.CLITest.Agent,
       control: [socket: path],
       channels: [console: [type: :console, adapter: Adapters.Local]]}
    )

    :ok = Adapters.Local.attach(name, :console)
    on_exit(fn -> File.rm(path) end)

    %{gateway: name, path: path}
  end

  test "parses options and selects VM or socket mode", %{gateway: gateway, path: path} do
    assert CLI.switches()[:gateway] == :string

    assert {[gateway: "beam_cli_test", timeout: 12, sender: "me"], ["console:one", "hi"]} =
             CLI.parse([
               "--gateway",
               "beam_cli_test",
               "--timeout",
               "12",
               "--sender",
               "me",
               "console:one",
               "hi"
             ])

    assert {:ok, {:vm, ^gateway}} = CLI.connect(gateway: Atom.to_string(gateway))
    assert :ok = CLI.disconnect({:vm, gateway})

    assert {:ok, {:socket, conn}} = CLI.connect(socket: path)
    assert :ok = CLI.disconnect({:socket, conn})
  end

  test "runs all finite VM commands", %{gateway: gateway} do
    assert capture_io(fn -> CLI.status({:vm, gateway}) end) =~ "gateway beam_cli_test"
    assert capture_io(fn -> CLI.doctor({:vm, gateway}) end) =~ "checks"

    assert capture_io(fn ->
             CLI.ask({:vm, gateway}, "console:asked", "question", sender: "cli", timeout: 5_000)
           end) != ""

    assert capture_io(fn -> CLI.push({:vm, gateway}, "console:pushed", "notice", []) end) =~
             "queued on console:pushed"

    chat =
      capture_io("/who\n/history 3\n/endpoint console\n/new\n/help\n/nope\n/stop\n/exit\n", fn ->
        CLI.chat({:vm, gateway}, "console:chat", endpoint: "console", timeout: 50)
      end)

    assert chat =~ "beam · console:chat"
    assert chat =~ "unknown command"

    tailer = Task.async(fn -> CLI.tail({:vm, gateway}, "console:vm-tail", limit: 1) end)

    assert_eventually(fn ->
      Enum.any?(Gateway.conversations(gateway), &(Spectre.Beam.Ref.slug(&1) == "console:vm-tail"))
    end)

    _stopped = Task.shutdown(tailer, :brutal_kill)
  end

  test "runs socket status, doctor, ask, push and interactive chat", %{path: path} do
    {:ok, conn} = Client.connect(path)
    on_exit(fn -> Client.close(conn) end)
    mode = {:socket, conn}

    assert capture_io(fn -> CLI.status(mode) end) =~ "gateway beam_cli_test"
    assert capture_io(fn -> CLI.doctor(mode) end) =~ "checks"

    assert capture_io(fn -> CLI.ask(mode, "console:socket-ask", "question", sender: "cli") end) !=
             ""

    assert capture_io(fn -> CLI.push(mode, "console:socket-push", "notice", timeout: 5_000) end) =~
             "queued on console:socket-push"

    output = capture_io("\nquestion\n/quit\n", fn -> CLI.chat(mode, nil, timeout: 5_000) end)
    assert output =~ "over control socket"
    assert output =~ "bot ›"
  end

  test "reports command and gateway errors", %{gateway: gateway} do
    assert capture_io(:stderr, fn ->
             assert catch_exit(CLI.push({:vm, gateway}, "missing:one", "hi", [])) ==
                      {:shutdown, 1}
           end) =~ "unknown_beam_endpoint"

    other = :beam_cli_other
    start_supervised!({Gateway, name: other, channels: []})

    assert {:error, {:ambiguous_beam_gateway, names}} = CLI.connect([])
    assert gateway in names and other in names
  end

  test "tails socket events and reports socket command failures", %{path: path} do
    {:ok, conn} = Client.connect(path)

    closer =
      Task.async(fn ->
        Process.sleep(10)
        {:ok, writer} = Client.connect(path)

        {:ok, _} =
          Client.request(writer, "send", %{"ref" => "console:tail", "text" => "question"})

        Client.close(writer)
        Process.sleep(500)
        Client.close(conn)
      end)

    output = capture_io(fn -> assert :ok = CLI.tail({:socket, conn}, "console:tail", []) end)
    Task.await(closer)
    assert output =~ "following console:tail"
    assert output =~ "socket › question"
    assert output =~ "bot ›"

    for command <- [:status, :doctor] do
      {:ok, closed} = Client.connect(path)
      :ok = Client.close(closed)

      assert capture_io(:stderr, fn ->
               assert catch_exit(apply(CLI, command, [{:socket, closed}])) == {:shutdown, 1}
             end) =~ "beam:"
    end

    for {command, args} <- [
          {:ask, ["missing:one", "hi", []]},
          {:push, ["missing:one", "hi", []]}
        ] do
      {:ok, socket} = Client.connect(path)
      on_exit(fn -> Client.close(socket) end)

      assert capture_io(:stderr, fn ->
               assert catch_exit(apply(CLI, command, [{:socket, socket} | args])) ==
                        {:shutdown, 1}
             end) =~ "unknown_beam_endpoint"
    end
  end

  test "Mix task entry points validate and dispatch", %{gateway: gateway} do
    gateway_arg = Atom.to_string(gateway)

    assert capture_task("beam.status", ["--gateway", gateway_arg]) =~ "gateway beam_cli_test"
    assert capture_task("beam.doctor", ["--gateway", gateway_arg]) =~ "checks"

    assert capture_task("beam.send", ["--gateway", gateway_arg, "console:mix", "question"]) !=
             ""

    assert capture_task("beam.send", ["--push", "--gateway", gateway_arg, "console:mix", "hi"]) =~
             "queued on console:mix"

    assert capture_io("/exit\n", fn ->
             rerun("beam.chat", ["--gateway", gateway_arg, "console:mix-chat"])
           end) =~ "beam · console:mix-chat"

    tailer =
      Task.async(fn ->
        rerun("beam.tail", ["--gateway", gateway_arg, "console:mix-tail", "--limit", "1"])
      end)

    assert_eventually(fn ->
      Enum.any?(
        Gateway.conversations(gateway),
        &(Spectre.Beam.Ref.slug(&1) == "console:mix-tail")
      )
    end)

    _stopped = Task.shutdown(tailer, :brutal_kill)

    assert_raise Mix.Error, ~r/usage: mix beam.send/, fn -> rerun("beam.send", []) end
    assert_raise Mix.Error, ~r/usage: mix beam.tail/, fn -> rerun("beam.tail", []) end
  end

  defp capture_task(task, args), do: capture_io(fn -> rerun(task, args) end)

  defp rerun(task, args) do
    Mix.Task.reenable(task)
    Mix.Task.run(task, args)
  end

  defp assert_eventually(fun, attempts \\ 20)
  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end
end
