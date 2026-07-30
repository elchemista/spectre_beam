defmodule Spectre.Beam.StackContractTest.Telegram do
  @moduledoc false
end

defmodule Spectre.Beam.StackContractTest.WhatsApp do
  @moduledoc false
end

defmodule Spectre.Beam.StackContractTest.Stack do
  @moduledoc false

  use Spectre.Stack, id: :beam_contract

  install Spectre.Beam, delivery: :caller_owned do
    channel(:telegram, Spectre.Beam.StackContractTest.Telegram)
    channel(:whatsapp, Spectre.Beam.StackContractTest.WhatsApp)
  end
end

defmodule Spectre.Beam.StackContractTest.Agent do
  @moduledoc false

  use Spectre.Agent, stack: Spectre.Beam.StackContractTest.Stack

  flow :telegram_only, beam: :telegram do
    on :hello, regex: ~r/^hello$/ do
      run(:telegram_reply)
    end
  end

  flow :notify do
    on :notify, regex: ~r/^notify$/ do
      beam(:owner, via: :telegram, text: "ready")
    end
  end

  def telegram_reply(_input, _context), do: "telegram"
end

defmodule Spectre.Beam.StackContractTest do
  use ExUnit.Case, async: true

  alias Spectre.Beam.StackContractTest.Agent
  alias Spectre.Beam.StackContractTest.Stack
  alias Spectre.Stack.Contract.V1
  alias Spectre.Stack.Definition
  alias Spectre.Stack.Runtime

  test "publishes a compatible V1 manifest with the Beam binding" do
    assert {:ok, package} = V1.verify_installable(Spectre.Beam)

    assert package.id == :beam
    assert package.version == "0.1.5"
    assert package.contract == 1
    assert package.spectre == "~> 0.1.5"
    assert package.dsl == Spectre.Beam
    assert package.provides == [{:service, :beam}]
    assert package.operations == []
    assert package.actions == []
    assert package.resources == []
    assert package.agent_extensions == [Spectre.Beam.Extension]
  end

  test "selecting the Stack activates Beam without use Spectre.Beam" do
    assert {:ok, mount} = Spectre.Extension.fetch(Agent, :beam)
    assert mount.module == Spectre.Beam.Extension
    assert mount.opts[:stack] == Stack

    assert {:ok, config} = Spectre.Beam.config(Agent)
    assert Enum.map(config.endpoints, & &1.id) == [:telegram, :whatsapp]
    assert Agent.__spectre_definition__().config[:beam] == config

    assert {:ok, stack_config} = Spectre.Stack.config(Agent, :beam)

    assert stack_config ==
             Spectre.Stack.installation(Agent, :beam) |> elem(1) |> Map.fetch!(:config)
  end

  test "Stack-only Beam source constraints and handler forms are executable" do
    input =
      Spectre.Input.new(%{
        text: "hello",
        source: %{kind: :beam, mount: :telegram}
      })

    assert {:ok, inbound} = Spectre.ask(Agent, input)
    assert inbound.route.flow == :telegram_only
    assert inbound.reply_text == "telegram"

    assert {:ok, outbound} = Spectre.ask(Agent, "notify")

    assert [%Spectre.Effect{kind: :action, status: :pending} = effect] =
             outbound.effects

    assert Spectre.Effect.via(effect) == {:beam, :telegram}
    assert effect.name == :send_text
    assert effect.args == %{text: "ready", to: :owner}
  end

  test "compiles ordered channel declarations inside a real Stack" do
    assert {:ok, installation} = Definition.installation(Stack, :beam)

    assert installation.config == %{
             options: [delivery: :caller_owned],
             channels: [
               {:telegram, Spectre.Beam.StackContractTest.Telegram},
               {:whatsapp, Spectre.Beam.StackContractTest.WhatsApp}
             ]
           }

    assert Stack.__spectre_stack_manifest__() == Definition.manifest(Stack)
  end

  test "rejects duplicate channel identifiers" do
    block =
      quote do
        channel(:telegram, Spectre.Beam.StackContractTest.Telegram)
        channel(:telegram, Spectre.Beam.StackContractTest.WhatsApp)
      end

    assert {:error, {:duplicate_beam_channel, :telegram}} =
             Spectre.Beam.compile([], block, __ENV__)
  end

  test "keeps provider clients caller-owned while resolving the Beam service" do
    assert {:ok, []} = Runtime.child_specs(Stack)

    assert {:ok, ref} = Definition.resolve(Stack, :service, :beam)
    assert ref.package == :beam
  end
end
