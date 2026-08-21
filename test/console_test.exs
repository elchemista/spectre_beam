defmodule Spectre.Beam.ConsoleTest.Model do
  @moduledoc false

  def complete(_prompt, _opts), do: {:ok, "unused"}
end

defmodule Spectre.Beam.ConsoleTest.Agent do
  @moduledoc false

  use Spectre.Agent, prompt_root: "test/fixtures/prompts"

  model(Spectre.Beam.ConsoleTest.Model)
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

defmodule Spectre.Beam.ConsoleTest.DecodeOnlyAdapter do
  @moduledoc false
  def decode(_event, _opts), do: :ignore
end

defmodule Spectre.Beam.ConsoleTest.EmptyAdapter do
  @moduledoc false
end

defmodule Spectre.Beam.ConsoleTest.BadStore do
  @moduledoc false
  def claim(_key, opts), do: Keyword.fetch!(opts, :result)
  def release(_key, _opts), do: :ok
end

defmodule Spectre.Beam.ConsoleTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Spectre.Beam.Adapters
  alias Spectre.Beam.Chat
  alias Spectre.Beam.Console
  alias Spectre.Beam.Doctor
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.IEx, as: BeamIEx
  alias Spectre.Beam.Ref

  setup do
    name = :"beam_console_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Gateway,
       name: name,
       agent: Spectre.Beam.ConsoleTest.Agent,
       channels: [console: [type: :console, adapter: Adapters.Local]]}
    )

    :ok = Adapters.Local.attach(name, :console)
    %{gateway: name}
  end

  describe "Console.open/2" do
    test "prefers a local channel and names a fresh conversation", %{gateway: gateway} do
      assert {:ok, ref} = Console.open(nil, gateway: gateway)
      assert ref.endpoint == :console
      assert String.starts_with?(Ref.slug(ref), "console:console-")
    end

    test "accepts an explicit address", %{gateway: gateway} do
      assert {:ok, ref} = Console.open("console:named", gateway: gateway)
      assert Ref.slug(ref) == "console:named"
    end

    test "refuses an endpoint the gateway does not mount", %{gateway: gateway} do
      assert {:error, {:unknown_beam_endpoint, :missing}} =
               Console.open(nil, gateway: gateway, endpoint: :missing)
    end

    test "reports an ambiguous gateway rather than guessing" do
      case Gateway.list() do
        [_only] ->
          assert {:error, :ambiguous_beam_gateway} != Console.open(nil, [])

        many when length(many) > 1 ->
          assert {:error, {:ambiguous_beam_gateway, _}} = Console.open(nil, [])
      end
    end
  end

  describe "Console.tail/2" do
    test "prints the retained transcript and then returns at the timeout", %{gateway: gateway} do
      {:ok, ref} = Chat.open(gateway, "console:tailed")
      {:ok, reply} = Chat.ask(ref, "question", timeout: 5_000)

      output = capture_io(fn -> Console.tail(ref, timeout: 50) end)

      assert output =~ "following console:tailed"
      assert output =~ reply
    end

    test "prints live delta, error and policy events", %{gateway: gateway} do
      {:ok, ref} = Chat.open(gateway, "console:events")

      Task.start(fn ->
        Process.sleep(10)
        bus = Spectre.Beam.Bus.default()
        Spectre.Beam.Bus.publish(bus, Spectre.Beam.Event.new(:delta, ref, %{text: "chunk"}))

        Spectre.Beam.Bus.publish(
          bus,
          Spectre.Beam.Event.new(:error, ref, %{stage: :turn, reason: :failed})
        )

        Spectre.Beam.Bus.publish(
          bus,
          Spectre.Beam.Event.new(:policy_required, ref, %{policy: :confirm})
        )
      end)

      output = capture_io(fn -> Console.tail(ref, timeout: 50) end)
      assert output =~ "chunk"
      assert output =~ "approval required"
    end
  end

  describe "IEx helpers" do
    test "say opens a conversation, remembers it, and prints the reply", %{gateway: gateway} do
      output = capture_io(fn -> BeamIEx.say("console:helper", "question") end)

      assert output != ""
      assert %Ref{} = BeamIEx.current()
      assert Ref.slug(BeamIEx.current()) == "console:helper"
      assert Ref.slug(BeamIEx.focus("console:other")) == "console:other"
      refute is_nil(gateway)
    end

    test "ask returns the reply text", %{gateway: _gateway} do
      assert {:ok, reply} = BeamIEx.ask("console:asked", "question")
      assert is_binary(reply)
    end

    test "endpoints and ls print one line each", %{gateway: gateway} do
      {:ok, _ref} = Chat.open(gateway, "console:listed")

      assert capture_io(fn -> BeamIEx.endpoints(gateway) end) =~ "console"
      assert capture_io(fn -> BeamIEx.ls(gateway) end) =~ "console:listed"
    end

    test "history prints the retained transcript", %{gateway: gateway} do
      {:ok, ref} = Chat.open(gateway, "console:historied")
      {:ok, _reply} = Chat.ask(ref, "question", timeout: 5_000)

      assert capture_io(fn -> BeamIEx.history(ref) end) =~ "bot ›"
    end

    test "gateways lists the running gateway", %{gateway: gateway} do
      assert gateway in BeamIEx.gateways()
    end

    test "implicit current helpers, push, status, cancel, tail and chat", %{gateway: _gateway} do
      assert capture_io(fn -> BeamIEx.say("question") end) != ""
      assert {:ok, reply} = BeamIEx.ask("question")
      assert is_binary(reply)

      ref = BeamIEx.current()
      assert {:ok, ^ref} = BeamIEx.push(ref, "notice")
      assert {:ok, %{status: :idle}} = BeamIEx.status()
      assert :ok = BeamIEx.cancel()

      assert capture_io(fn -> BeamIEx.tail(nil, timeout: 10, limit: 1) end) =~ "following"
      assert capture_io("/quit\n", fn -> BeamIEx.chat(ref) end) =~ Ref.slug(ref)
    end

    test "prints resolution errors instead of crashing", %{gateway: gateway} do
      other = :"beam_console_other_#{System.unique_integer([:positive])}"
      start_supervised!({Gateway, name: other, channels: []})

      assert capture_io(:stderr, fn -> BeamIEx.endpoints() end) =~ "ambiguous_beam_gateway"
      assert capture_io(:stderr, fn -> BeamIEx.ls() end) =~ "ambiguous_beam_gateway"
      assert {:error, {:ambiguous_beam_gateway, _}} = BeamIEx.focus("console:nope")

      assert capture_io(:stderr, fn -> BeamIEx.say("console:nope", "question") end) =~
               "no reply"

      refute is_nil(gateway)
    end
  end

  describe "interactive transport-only console" do
    test "reports missing configuration and a transport-only turn" do
      empty = :"beam_empty_#{System.unique_integer([:positive])}"
      start_supervised!({Gateway, name: empty, channels: []})
      assert Console.open(nil, gateway: empty) == {:error, :no_beam_endpoint_configured}

      transport = :"beam_transport_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Gateway, name: transport, channels: [console: [type: :console, adapter: Adapters.Local]]}
      )

      output =
        capture_io("\nhello\n/exit\n", fn ->
          Console.chat("console:transport", gateway: transport, timeout: 100)
        end)

      assert output =~ "transport-only gateway"
    end

    test "prints an open error" do
      assert capture_io(:stderr, fn ->
               Console.chat(nil, gateway: :definitely_missing_gateway)
             end) =~ "cannot open a conversation"
    end
  end

  describe "Doctor" do
    test "passes on a healthy gateway", %{gateway: gateway} do
      checks = Doctor.run(gateway)

      assert Doctor.verdict(checks) in [:ok, :warn]
      refute Enum.any?(checks, &(&1.status == :error))
      assert Enum.any?(checks, &(&1.check == :adapter and &1.status == :ok))
      assert Enum.any?(checks, &(&1.check == :outbox and &1.status == :ok))
      assert Enum.any?(checks, &(&1.check == :store and &1.status == :ok))
    end

    test "reports a gateway that is not running" do
      assert [%{check: :gateway, status: :error}] =
               :never_started |> Doctor.run() |> Enum.filter(&(&1.scope == :never_started))
    end

    test "flags instance scope without a supervisor" do
      name = :"beam_doctor_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Gateway,
         name: name,
         agent: Spectre.Beam.ConsoleTest.Agent,
         scope: :instance,
         channels: [console: [type: :console, adapter: Adapters.Local]]}
      )

      checks = Doctor.run(name)

      assert Enum.any?(checks, &(&1.check == :scope and &1.status == :error))
      assert Doctor.verdict(checks) == :error
    end

    test "prints a readable report", %{gateway: gateway} do
      output = capture_io(fn -> Doctor.report(gateway) end)

      assert output =~ "checks"
      assert output =~ "console"
    end

    test "diagnoses optional agent, store, adapter and session misconfiguration" do
      cases = [
        [agent: MissingBeamAgent, store: {MissingBeamStore, []}],
        [agent: nil, store: {Spectre.Beam.ConsoleTest.Model, []}],
        [agent: nil, store: {Spectre.Beam.ConsoleTest.BadStore, [result: :busy]}],
        [agent: nil, store: {Spectre.Beam.ConsoleTest.BadStore, []}],
        [agent: nil, session: true]
      ]

      Enum.each(cases, fn extra ->
        name = :"beam_doctor_case_#{System.unique_integer([:positive])}"

        start_supervised!(
          {Gateway,
           [
             name: name,
             channels: [console: [type: :console, adapter: Adapters.Local]]
           ] ++ extra}
        )

        assert Doctor.verdict(Doctor.run(name)) in [:warn, :error]
      end)

      for adapter <- [
            MissingBeamAdapter,
            Spectre.Beam.ConsoleTest.EmptyAdapter,
            Spectre.Beam.ConsoleTest.DecodeOnlyAdapter
          ] do
        name = :"beam_doctor_adapter_#{System.unique_integer([:positive])}"
        start_supervised!({Gateway, name: name, channels: [bad: [adapter: adapter]]})
        assert Enum.any?(Doctor.run(name), &(&1.check == :adapter and &1.status == :error))
      end

      report = capture_io(fn -> Doctor.report(:never_started) end)
      assert report =~ "FAIL"
    end
  end
end
