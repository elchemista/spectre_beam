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
  end
end
