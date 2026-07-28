defmodule Spectre.Beam.AdapterPipelineTest.Provider do
  @moduledoc false

  def send_message(client, to, text) do
    send(client, {:provider_send, :text, to, text})
    {:ok, "sent-text"}
  end

  def send_message_await(client, to, text, _timeout) do
    send(client, {:provider_send, :await_text, to, text})

    if text == "timeout",
      do: {:error, :ack_timeout},
      else: {:ok, "ack-text"}
  end

  def send_document(client, to, source, opts) do
    send(client, {:provider_send, :document, to, source, opts})
    {:ok, "sent-document"}
  end

  def subscribe(client) do
    send(client, :provider_subscribed)
    :ok
  end

  def unsubscribe(client) do
    send(client, :provider_unsubscribed)
    :ok
  end
end

defmodule Spectre.Beam.AdapterPipelineTest.Adapter do
  @behaviour Spectre.Beam.Channel

  @impl true
  def decode(event, _opts) do
    {:ok,
     %{
       message_id: event.id,
       conversation_id: event.conversation,
       sender: event.sender,
       content: %{type: :text, text: event.text},
       authenticated?: true,
       metadata: %{}
     }}
  end

  @impl true
  def deliver(outbound, _opts), do: {:ok, Spectre.Beam.Receipt.accepted(outbound)}
end

defmodule Spectre.Beam.AdapterPipelineTest.SuffixPlug do
  @behaviour Spectre.Beam.Plug

  alias Spectre.Beam.Pipeline

  @impl true
  def init(opts), do: Keyword.fetch!(opts, :suffix)

  @impl true
  def call(%Pipeline{stage: :before_decode, value: event} = pipeline, suffix) do
    Pipeline.put_value(pipeline, %{event | text: event.text <> suffix})
  end

  def call(pipeline, _suffix), do: pipeline
end

defmodule Spectre.Beam.AdapterPipelineTest.MarkPlug do
  @behaviour Spectre.Beam.Plug

  alias Spectre.Beam.Pipeline

  @impl true
  def call(%Pipeline{stage: :after_decode, value: inbound} = pipeline, _opts) do
    marked = %{inbound | metadata: Map.put(inbound.metadata, :pipeline, :ran)}
    Pipeline.put_value(pipeline, marked)
  end

  def call(pipeline, _opts), do: pipeline
end

defmodule Spectre.Beam.AdapterPipelineTest.EndpointMutationPlug do
  @behaviour Spectre.Beam.Plug

  alias Spectre.Beam.Pipeline

  @impl true
  def call(%Pipeline{} = pipeline, _opts) do
    %{pipeline | endpoint: %{pipeline.endpoint | opts: [client: self()]}}
  end
end

defmodule Spectre.Beam.AdapterPipelineTest.Agent do
  @moduledoc false

  use Spectre.Agent
  use Spectre.Beam

  beaming do
    channel(:test,
      adapter: Spectre.Beam.AdapterPipelineTest.Adapter,
      before_decode: [{Spectre.Beam.AdapterPipelineTest.SuffixPlug, suffix: "!"}],
      inbound_pipeline: [Spectre.Beam.AdapterPipelineTest.MarkPlug]
    )
  end
end

defmodule Spectre.Beam.AdapterPipelineTest do
  use ExUnit.Case, async: true

  alias Spectre.Beam.Adapters.ExGram
  alias Spectre.Beam.Adapters.ExWapp
  alias Spectre.Beam.AdapterPipelineTest.Agent
  alias Spectre.Beam.AdapterPipelineTest.Provider
  alias Spectre.Beam.Content
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Pipeline

  test "provider-neutral plugs transform both sides of decode in order" do
    event = %{id: "event-1", conversation: "chat-1", sender: "user-1", text: "hello"}

    assert {:ok, inbound} = Spectre.Beam.decode(Agent, :test, event)
    assert inbound.content.text == "hello!"
    assert inbound.metadata.pipeline == :ran
  end

  test "plugs cannot replace endpoint identity while retaining its id" do
    endpoint = Endpoint.new(:test, adapter: Spectre.Beam.AdapterPipelineTest.Adapter)

    assert {:error, {:beam_plug_changed_pipeline_identity, :before_decode}} =
             Pipeline.run(
               :before_decode,
               endpoint,
               %{},
               [Spectre.Beam.AdapterPipelineTest.EndpointMutationPlug]
             )
  end

  test "ExGram normalizes local PubSub messages without a compile dependency" do
    message = %{
      id: 101,
      chat_id: -100,
      sender_id: 42,
      from_me: false,
      timestamp: 1_774_348_400,
      kind: :text,
      text: "ciao",
      content: %{kind: :text, text: "ciao"}
    }

    assert {:ok, inbound} =
             ExGram.decode({:ex_gram_message, :telegram_session, "-100", message}, [])

    assert inbound.message_id == "101"
    assert inbound.conversation_id == "-100"
    assert inbound.sender == 42
    assert inbound.content.type == :text
    assert inbound.content.text == "ciao"
    assert inbound.metadata.provider == :ex_gram
  end

  test "ExWapp normalizes typed media references without downloading content" do
    media = %{type: :image, id: "encrypted-media-ref"}

    message = %{
      id: "wa-1",
      from_me: false,
      participant: "member@s.whatsapp.net",
      timestamp: 1_774_348_400,
      content: %{kind: :media, text: "caption", media: media}
    }

    assert {:ok, inbound} =
             ExWapp.decode({:ex_wapp_message, "group@g.us", message}, [])

    assert inbound.conversation_id == "group@g.us"
    assert inbound.sender == "member@s.whatsapp.net"
    assert inbound.content.type == :image
    assert inbound.content.text == "caption"
    assert inbound.content.data == media
  end

  test "ExGram delivery dynamically invokes a compatible provider module" do
    outbound =
      Outbound.new(%{
        endpoint: :telegram,
        to: 123,
        content: Content.new(%{type: :document, data: %{source: {:path, "/tmp/a.pdf"}}}),
        idempotency_key: "telegram-document-1"
      })

    assert {:ok, receipt} =
             ExGram.deliver(outbound, module: Provider, client: self(), send_opts: [caption: "A"])

    assert_receive {:provider_send, :document, 123, {:path, "/tmp/a.pdf"}, [caption: "A"]}
    assert receipt.provider_message_id == "sent-document"
  end

  test "ExWapp ack timeout is explicitly ambiguous" do
    outbound =
      Outbound.new(%{
        endpoint: :whatsapp,
        to: "user@s.whatsapp.net",
        content: Content.text("timeout"),
        idempotency_key: "whatsapp-text-1"
      })

    assert {:error, {:ambiguous, :ack_timeout}} =
             ExWapp.deliver(outbound,
               module: Provider,
               client: self(),
               await_ack: true,
               timeout: 10
             )

    assert_receive {:provider_send, :await_text, "user@s.whatsapp.net", "timeout"}
  end

  test "optional subscription lifecycle uses the same adapter protocol" do
    assert :ok = ExWapp.subscribe(module: Provider, client: self())
    assert_receive :provider_subscribed

    assert :ok = ExWapp.unsubscribe(module: Provider, client: self())
    assert_receive :provider_unsubscribed
  end
end
