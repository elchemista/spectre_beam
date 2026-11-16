defmodule Spectre.Beam.ServerToolTest do
  use ExUnit.Case, async: false
  alias Spectre.Beam.ServerTool

  test "managed delivery registration remains opt-in and uses a closed adapter" do
    assert %{} = Spectre.Beam.effects(%{options: [], channels: []})
    effects = Spectre.Beam.effects(%{options: [bind: [deliver: :send_message]], channels: []})
    assert %{send_message: %{adapter: ServerTool, operation: :deliver}} = effects
    assert Spectre.Stack.Effects.validate!(effects) == effects
  end

  defmodule Channel do
    def capabilities(_opts), do: [:text]

    def deliver(outbound, _opts) do
      send(self(), {:actual_delivery, outbound})

      if outbound.content.text == "ambiguous",
        do: {:error, {:ambiguous, self()}},
        else: {:ok, Spectre.Beam.Receipt.accepted(outbound, provider_message_id: "provider-1")}
    end
  end

  defmodule Stack do
    use Spectre.Stack

    install Spectre.Beam, bind: [deliver: :send_message] do
      channel(:mail, adapter: Channel)
    end
  end

  defmodule Server do
    use Spectre.Server
    def handle_flow(:unmatched, input, ctx), do: respond(ctx, "Received: " <> input.text)
    def handle_flow(route, input, ctx), do: super(route, input, ctx)
  end

  defmodule Agent do
    use Spectre.Agent, stack: Stack, server: Server
  end

  test "registered endpoints use the real channel capability adapter and return plain data" do
    {:ok, installation} = Spectre.Stack.installation(Agent, :beam)
    assert installation.adapters.tools.beam.operations.deliver.path == :governed
    assert installation.adapters.tools.beam.operations.deliver.operation == :send_message
    assert {:ok, %{}} = ServerTool.prepare(:endpoints, [], %{config: installation.config})
    result = ServerTool.invoke(:endpoints, %{}, %{installation: installation})

    assert {:ok, {:ok, [%{id: :mail, capabilities: [:text]}]}} =
             ServerTool.project(:endpoints, result)
  end

  test "prepare never delivers; admitted primitive injects stable id and projects receipt" do
    {:ok, installation} = Spectre.Stack.installation(Agent, :beam)
    attrs = %{to: "dev@example.com", content: %{type: :text, text: "Hello"}}

    assert {:ok, args} =
             ServerTool.prepare(:deliver, [:mail, attrs], %{config: installation.config})

    refute_received {:actual_delivery, _}

    id = Spectre.Identity.uuid7()

    assert {:ok, %{status: :accepted, outbound_id: ^id, occurred_at: date}} =
             ServerTool.execute_effect(:deliver, args, %{agent: Agent, command_id: id})

    assert is_binary(date)
    assert_received {:actual_delivery, %{idempotency_key: ^id}}
    assert {:ok, _} = ServerTool.execute_effect(:deliver, args, %{agent: Agent, command_id: id})
    refute_received {:actual_delivery, _}
  end

  test "registered executor rejects invalid endpoints and content before delivery" do
    attrs = %{to: "dev@example.com", content: %{type: :text, text: "Hello"}}
    context = %{agent: Agent, command_id: Spectre.Identity.uuid7()}

    assert {:error, :definitive_no_effect, {:unknown_beam_endpoint, :missing}} =
             ServerTool.execute_effect(:deliver, %{endpoint: :missing, outbound: attrs}, context)

    assert {:error, :definitive_no_effect, :invalid_delivery_request} =
             ServerTool.execute_effect(
               :deliver,
               %{endpoint: :mail, outbound: Map.delete(attrs, :content)},
               context
             )

    refute_received {:actual_delivery, _}
  end

  test "delivery binding is opt-in and tool input cannot replace transport options" do
    assert %{beam: %{operations: operations}} = Spectre.Beam.tools(%{options: [], channels: []})
    refute Map.has_key?(operations, :deliver)
    {:ok, installation} = Spectre.Stack.installation(Agent, :beam)

    assert {:error, :unsupported_delivery_fields} =
             ServerTool.prepare(
               :deliver,
               [
                 :mail,
                 %{to: "dev@example.com", content: %{type: :text, text: "Hi"}, adapter: Channel}
               ],
               %{config: installation.config}
             )
  end

  test "ambiguous delivery stays fenced and preserves uncertainty without runtime handles" do
    {:ok, installation} = Spectre.Stack.installation(Agent, :beam)
    attrs = %{to: "dev@example.com", content: %{type: :text, text: "ambiguous"}}
    {:ok, args} = ServerTool.prepare(:deliver, [:mail, attrs], %{config: installation.config})
    context = %{agent: Agent, command_id: Spectre.Identity.uuid7()}

    assert {:error, :ambiguous, :beam_delivery_error_details_unavailable} =
             ServerTool.execute_effect(:deliver, args, context)

    assert_received {:actual_delivery, _}

    assert {:error, :ambiguous, {:beam_delivery_in_progress, _}} =
             ServerTool.execute_effect(:deliver, args, context)

    refute_received {:actual_delivery, _}
  end

  test "decoded channel input queues on the real AgentServer and uses its response consumer" do
    server = start_supervised!({Spectre.Server, agent: Agent, consumer: self()})

    inbound =
      Spectre.Beam.Inbound.new(
        endpoint: :mail,
        message_id: "incoming-1",
        conversation_id: "contact-1",
        sender: "dev@example.com",
        authenticated?: true,
        content: %{type: :text, text: "Hi"}
      )

    assert {:ok, id} = Spectre.Beam.enqueue(server, inbound)
    assert_receive {Spectre.Server, ^server, {:response, ^id, "Received: Hi", metadata}}, 2000
    assert is_binary(metadata[:message_id])
  end

  test "binding typos, duplicates, nil and non-atom operations fail during installation" do
    for bindings <- [
          [delivre: :send],
          [deliver: nil],
          [deliver: "send"],
          [deliver: :one, deliver: :two],
          :invalid
        ] do
      assert_raise ArgumentError, ~r/beam bind:/, fn ->
        Spectre.Beam.tools(%{options: [bind: bindings], channels: []})
      end
    end
  end
end
