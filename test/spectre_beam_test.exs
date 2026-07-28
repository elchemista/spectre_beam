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

defmodule Spectre.Beam.StackContractTest do
  use ExUnit.Case, async: true

  alias Spectre.Beam.StackContractTest.Stack
  alias Spectre.Stack.Contract.V1
  alias Spectre.Stack.Definition
  alias Spectre.Stack.Runtime

  test "publishes a compatible V1 manifest without fake capabilities" do
    assert {:ok, package} = V1.verify_installable(Spectre.Beam)

    assert package.id == :beam
    assert package.version == "0.1.2"
    assert package.contract == 1
    assert package.spectre == "~> 0.1.2"
    assert package.dsl == Spectre.Beam
    assert package.provides == []
    assert package.operations == []
    assert package.actions == []
    assert package.resources == []
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

  test "does not invent runtime resources" do
    assert {:ok, []} = Runtime.child_specs(Stack)

    assert {:error, {:unknown_stack_capability, :service, :beam}} =
             Definition.resolve(Stack, :service, :beam)
  end
end
