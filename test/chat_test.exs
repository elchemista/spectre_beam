defmodule Spectre.Beam.ChatTest.Model do
  @moduledoc false

  def complete(_prompt, _opts), do: {:ok, "unused"}
end

defmodule Spectre.Beam.ChatTest.Agent do
  @moduledoc false

  use Spectre.Agent, prompt_root: "test/fixtures/prompts"

  model(Spectre.Beam.ChatTest.Model)
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

defmodule Spectre.Beam.ChatTest.StampPlug do
  @moduledoc false

  @behaviour Spectre.Beam.Plug

  alias Spectre.Beam.Pipeline

  @impl true
  def call(%Pipeline{stage: :after_decode, value: inbound} = pipeline, _opts) do
    Pipeline.put_value(pipeline, %{
      inbound
      | metadata: Map.put(inbound.metadata, :stamped, true)
    })
  end

  def call(pipeline, _opts), do: pipeline
end

defmodule Spectre.Beam.ZeroConfigAgent do
  @moduledoc false

  use Spectre.Agent, prompt_root: "test/fixtures/prompts"

  model(Spectre.Beam.ChatTest.Model)
  use Spectre.Beam

  flow :local_flow do
    on :question, regex: ~r/question/i do
      reply(:generic_reply)
    end
  end
end

defmodule Spectre.Beam.ChatTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Adapters
  alias Spectre.Beam.Chat
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Ref

  setup context do
    name = :"beam_chat_#{System.unique_integer([:positive])}"

    channel =
      [type: :console, adapter: Adapters.Local]
      |> Keyword.merge(Map.get(context, :channel, []))

    start_supervised!(
      {Gateway,
       name: name,
       agent: Map.get(context, :agent, Spectre.Beam.ChatTest.Agent),
       channels: [console: channel]}
    )

    :ok = Adapters.Local.attach(name, :console)
    %{gateway: name}
  end

  test "send is asynchronous and answers on the bus", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:one")
    :ok = Chat.subscribe(ref)

    assert {:ok, ^ref} = Chat.send(ref, "question")

    assert_receive %Event{type: :inbound, payload: %{text: "question"}}, 1_000
    assert_receive %Event{type: :typing, payload: %{composing?: true}}, 1_000
    assert_receive %Event{type: :reply, payload: %{text: text}}, 2_000
    assert_receive %Event{type: :typing, payload: %{composing?: false}}, 1_000
    assert_receive %Event{type: :status, payload: %{status: :idle}}, 1_000

    assert is_binary(text)
    assert {:ok, outbound} = Adapters.Test.next_delivery(2_000)
    assert outbound.content.text == text
  end

  test "ask blocks and returns the reply text", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:two")

    assert {:ok, reply} = Chat.ask(ref, "question", timeout: 5_000)
    assert is_binary(reply)
  end

  test "ask does not disturb an existing subscription", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:three")
    :ok = Chat.subscribe(ref)

    assert {:ok, _reply} = Chat.ask(ref, "question", timeout: 5_000)

    assert_receive %Event{type: :inbound}, 1_000
    assert_receive %Event{type: :reply}, 1_000
  end

  test "history replays from a cursor without gaps", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:four")

    {:ok, _reply} = Chat.ask(ref, "question", timeout: 5_000)
    first = Chat.history(ref)

    assert length(first) > 1
    assert Enum.map(first, & &1.seq) == Enum.sort(Enum.map(first, & &1.seq))

    cursor = first |> List.last() |> Map.fetch!(:seq)
    assert Chat.history(ref, after: cursor) == []

    {:ok, _second} = Chat.ask(ref, "question", timeout: 5_000)
    resumed = Chat.history(ref, after: cursor)

    assert resumed != []
    assert Enum.all?(resumed, &(&1.seq > cursor))
  end

  test "history filters by event type", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:five")
    {:ok, _reply} = Chat.ask(ref, "question", timeout: 5_000)

    assert [%Event{type: :reply}] = Chat.history(ref, types: [:reply])
  end

  @tag channel: [inbound_pipeline: [Spectre.Beam.ChatTest.StampPlug]]
  test "a locally injected message still runs the inbound pipeline", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:six")
    :ok = Chat.subscribe(ref)

    assert {:ok, ^ref} = Chat.send(ref, "question")
    assert_receive %Event{type: :inbound, payload: %{inbound: inbound}}, 1_000
    assert inbound.metadata.stamped
  end

  test "push delivers without producing a turn", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:seven")
    :ok = Chat.subscribe(ref)

    assert {:ok, ^ref} = Chat.push(ref, "sto uscendo")
    assert {:ok, outbound} = Adapters.Test.next_delivery(1_000)
    assert outbound.content.text == "sto uscendo"

    refute_receive %Event{type: :inbound}, 200
  end

  test "cancel stops a turn in flight", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:eight")
    :ok = Chat.subscribe(ref)

    {:ok, ^ref} = Chat.send(ref, "question")
    :ok = Chat.cancel(ref)

    assert {:ok, status} = Chat.status(ref)
    assert status.status == :idle
  end

  @tag agent: nil
  test "reports a transport-only gateway instead of hanging", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:nine")

    assert {:error, :beam_transport_only} = Chat.ask(ref, "question", timeout: 2_000)
  end

  test "close drops the conversation and its transcript", %{gateway: gateway} do
    {:ok, ref} = Chat.open(gateway, "console:ten")
    {:ok, _reply} = Chat.ask(ref, "question", timeout: 5_000)

    assert Chat.history(ref) != []
    assert :ok = Chat.close(ref)
    assert Chat.history(ref) == []
    assert Gateway.conversations(gateway) == []
    assert {:error, :not_found} = Chat.status(Ref.parse!("console:ten", gateway: gateway))
  end
end

defmodule Spectre.Beam.ZeroConfigChatTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Adapters.Local
  alias Spectre.Beam.Chat
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.ZeroConfigAgent

  setup do
    on_exit(fn -> Gateway.stop(ZeroConfigAgent) end)
    :ok
  end

  test "use Spectre.Beam alone provides a supervised local conversation" do
    assert {:ok, config} = Spectre.Beam.config(ZeroConfigAgent)
    assert [%{id: :local, adapter: Local}] = config.endpoints

    assert {:ok, ref} = Chat.open(ZeroConfigAgent, conversation: "live-view-42")
    assert ref.gateway == ZeroConfigAgent
    assert ref.endpoint == :local
    assert ref.conversation_id == "live-view-42"
    assert {:ok, _spec} = Gateway.spec(ZeroConfigAgent)

    :ok = Local.attach(ZeroConfigAgent, :local)
    assert {:ok, reply} = Chat.ask(ref, "question", timeout: 5_000)
    assert is_binary(reply)
  end

  test "the top-level helper and IEx helper accept the Agent module directly" do
    assert {:ok, ref} = Chat.open(ZeroConfigAgent)
    :ok = Local.attach(ZeroConfigAgent, :local)

    assert {:ok, reply} = Spectre.Beam.ask(ZeroConfigAgent, "question", timeout: 5_000)
    assert is_binary(reply)

    assert {:ok, iex_reply} =
             Spectre.Beam.IEx.ask(ZeroConfigAgent, "question", timeout: 5_000)

    assert is_binary(iex_reply)
    assert ref.gateway == ZeroConfigAgent
  end
end
