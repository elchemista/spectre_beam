defmodule Spectre.Beam.GatewayTest.Model do
  @moduledoc false

  def complete(_prompt, _opts), do: {:ok, "unused"}
end

defmodule Spectre.Beam.GatewayTest.Agent do
  @moduledoc false

  use Spectre.Agent, prompt_root: "test/fixtures/prompts"

  model(Spectre.Beam.GatewayTest.Model)
  use Spectre.Beam

  beaming do
    channel(:chat, type: :test, adapter: Spectre.Beam.Adapters.Test)
  end

  flow :chat_flow do
    on :question, regex: ~r/question/i do
      reply(:generic_reply)
    end
  end
end

defmodule Spectre.Beam.GatewayTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Adapters
  alias Spectre.Beam.Bus
  alias Spectre.Beam.Config
  alias Spectre.Beam.Conversation
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Outbox
  alias Spectre.Beam.Ref

  defp start_gateway(opts) do
    name = :"beam_gw_#{System.unique_integer([:positive])}"

    channel_opts =
      Keyword.merge(
        [type: :test, adapter: Adapters.Test],
        Keyword.get(opts, :channel, [])
      )

    start_supervised!(
      {Gateway,
       Keyword.merge(
         [name: name, channels: [chat: channel_opts]],
         Keyword.drop(opts, [:channel])
       )}
    )

    :ok = Adapters.Test.attach(name, :chat)
    name
  end

  defp subscribe(gateway, slug) do
    {:ok, ref} = Gateway.open(gateway, slug)
    :ok = Bus.subscribe(Bus.default(), Ref.topic(ref))
    ref
  end

  describe "transport-only mode" do
    test "publishes normalized inbound without reaching Spectre" do
      gateway = start_gateway([])
      ref = subscribe(gateway, "chat:demo")

      assert {:ok, ^ref} =
               Gateway.ingest(gateway, :chat, %{text: "ciao", conversation_id: "demo"})

      assert_receive %Event{type: :inbound, payload: %{text: "ciao"}}, 500
      assert_receive %Event{type: :status, payload: %{status: :transport_only}}, 500
    end

    test "stamps a monotonic sequence on every event" do
      gateway = start_gateway([])
      ref = subscribe(gateway, "chat:seq")

      {:ok, ^ref} = Gateway.ingest(gateway, :chat, %{text: "one", conversation_id: "seq"})
      assert_receive %Event{type: :inbound, seq: first}, 500
      assert_receive %Event{type: :status, seq: second}, 500

      assert second > first
    end
  end

  describe "compiled Beam configuration" do
    test "starts and delivers with a reused config and no channel declarations" do
      gateway = :"beam_reused_#{System.unique_integer([:positive])}"
      beam = Config.new([Endpoint.new(:chat, type: :test, adapter: Adapters.Test)])

      start_supervised!({Gateway, name: gateway, beam: beam})
      :ok = Adapters.Test.attach(gateway, :chat)

      assert [%{endpoint: :chat, status: :up}] = Gateway.endpoints(gateway)
      assert {:ok, _ref} = Gateway.push(gateway, "chat:reuse", "hello")
      assert {:ok, %{content: %{text: "hello"}}} = Adapters.Test.next_delivery()
    end
  end

  describe "inbound deduplication" do
    test "recognizes a provider redelivery of the same message" do
      gateway = start_gateway([])
      ref = subscribe(gateway, "chat:dedup")

      event = %{text: "hello", conversation_id: "dedup", message_id: "m-1"}

      assert {:ok, ^ref} = Gateway.ingest(gateway, :chat, event)
      assert_receive %Event{type: :inbound}, 500

      assert {:duplicate, ^ref} = Gateway.ingest(gateway, :chat, event)
      refute_receive %Event{type: :inbound}, 100
    end

    test "can be disabled per call" do
      gateway = start_gateway([])
      ref = subscribe(gateway, "chat:nodedup")

      event = %{text: "hello", conversation_id: "nodedup", message_id: "m-1"}

      assert {:ok, ^ref} = Gateway.ingest(gateway, :chat, event)
      assert {:ok, ^ref} = Gateway.ingest(gateway, :chat, event, deduplicate: false)

      assert_receive %Event{type: :inbound}, 500
      assert_receive %Event{type: :inbound}, 500
    end
  end

  describe "agent turns" do
    test "answers through the endpoint outbox" do
      gateway = start_gateway(agent: Spectre.Beam.GatewayTest.Agent)
      ref = subscribe(gateway, "chat:agent")

      assert {:ok, ^ref} =
               Gateway.ingest(gateway, :chat, %{text: "question", conversation_id: "agent"})

      assert_receive %Event{type: :status, payload: %{status: :running}}, 1_000
      assert_receive %Event{type: :reply, payload: %{text: text}}, 2_000
      assert is_binary(text)

      assert {:ok, outbound} = Adapters.Test.next_delivery(2_000)
      assert outbound.content.text == text
      assert outbound.metadata.kind == :reactive

      assert_receive %Event{type: :receipt, payload: %{receipt: receipt}}, 2_000
      assert receipt.status == :accepted
    end

    test "serializes concurrent messages on one conversation" do
      gateway = start_gateway(agent: Spectre.Beam.GatewayTest.Agent)
      ref = subscribe(gateway, "chat:serial")

      for index <- 1..3 do
        {:ok, ^ref} =
          Gateway.ingest(gateway, :chat, %{
            text: "question #{index}",
            conversation_id: "serial",
            message_id: "m-#{index}"
          })
      end

      assert {:ok, _first} = Adapters.Test.next_delivery(2_000)
      assert {:ok, _second} = Adapters.Test.next_delivery(2_000)
      assert {:ok, _third} = Adapters.Test.next_delivery(2_000)

      assert {:ok, status} = Conversation.status(ref)
      assert status.turns == 3
      assert status.status == :idle
    end

    test "coalesces a burst into one turn" do
      gateway = start_gateway(agent: Spectre.Beam.GatewayTest.Agent, channel: [coalesce_ms: 60])
      ref = subscribe(gateway, "chat:burst")

      for index <- 1..3 do
        {:ok, ^ref} =
          Gateway.ingest(gateway, :chat, %{
            text: "question #{index}",
            conversation_id: "burst",
            message_id: "b-#{index}"
          })
      end

      assert {:ok, _outbound} = Adapters.Test.next_delivery(2_000)
      assert {:error, :timeout} = Adapters.Test.next_delivery(300)

      assert {:ok, status} = Conversation.status(ref)
      assert status.turns == 1
    end
  end

  describe "proactive delivery" do
    test "pushes without blocking the caller" do
      gateway = start_gateway([])
      ref = subscribe(gateway, "chat:push")

      assert {:ok, ^ref} = Gateway.push(gateway, "chat:push", "il report è pronto")

      assert {:ok, outbound} = Adapters.Test.next_delivery(1_000)
      assert outbound.content.text == "il report è pronto"
      assert outbound.metadata.kind == :proactive

      assert_receive %Event{type: :receipt}, 1_000
      assert :ok = Outbox.drain(gateway, :chat)
    end

    test "reports an unknown endpoint instead of raising" do
      gateway = start_gateway([])
      assert {:error, {:unknown_beam_endpoint, "nope"}} = Gateway.push(gateway, "nope:1", "hi")
    end
  end

  describe "introspection" do
    test "lists endpoints and conversations" do
      gateway = start_gateway([])
      ref = subscribe(gateway, "chat:list")

      assert [%{endpoint: :chat, status: :up, ingress: :none}] = Gateway.endpoints(gateway)
      assert [listed] = Gateway.conversations(gateway)
      assert Ref.slug(listed) == Ref.slug(ref)

      assert :ok = Gateway.close(gateway, ref)
      assert Gateway.conversations(gateway) == []
    end
  end
end
