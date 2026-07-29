defmodule Spectre.Beam.BoundaryContractTest.Provider do
  @moduledoc false

  def send_message(client, to, text),
    do: respond(client, {:provider_call, :text, to, text}, {:ok, "text-id"})

  def send_message_await(client, to, text, timeout) do
    reply = if text == "timeout", do: {:error, :ack_timeout}, else: {:ok, "ack-id"}
    respond(client, {:provider_call, :await_text, to, text, timeout}, reply)
  end

  def send_document(client, to, source, opts),
    do: respond(client, {:provider_call, :document, to, source, opts}, {:ok, "document-id"})

  def send_location(client, to, latitude, longitude, opts),
    do:
      respond(
        client,
        {:provider_call, :location, to, latitude, longitude, opts},
        {:ok, "location-id"}
      )

  def send_contact(client, to, display_name, vcard, opts),
    do:
      respond(
        client,
        {:provider_call, :contact, to, display_name, vcard, opts},
        {:ok, "contact-id"}
      )

  def send_event(client, to, name, start_time, opts),
    do:
      respond(
        client,
        {:provider_call, :event, to, name, start_time, opts},
        {:ok, "event-id"}
      )

  def subscribe(client), do: respond(client, :provider_subscribed, :ok)
  def unsubscribe(client), do: respond(client, :provider_unsubscribed, :ok)

  defp respond({pid, response}, message, _default) do
    send(pid, message)
    response
  end

  defp respond(pid, message, default) when is_pid(pid) do
    send(pid, message)
    default
  end
end

defmodule Spectre.Beam.BoundaryContractTest.RuntimeAdapter do
  @moduledoc false

  alias Spectre.Beam.Content
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Receipt

  def capabilities(opts) do
    case Keyword.get(opts, :capability_reply, [:text, :document, :location, :contact, :event]) do
      :raise -> raise "capabilities failed"
      :throw -> throw(:capabilities_failed)
      reply -> reply
    end
  end

  def decode(event, opts) do
    case Keyword.get(opts, :decode, :default) do
      :default ->
        {:ok,
         %Inbound{
           message_id: Map.get(event, :id, "message-1"),
           conversation_id: Map.get(event, :conversation, "conversation-1"),
           sender: Map.get(event, :sender, "sender-1"),
           content: Content.text(Map.get(event, :text, "hello")),
           authenticated?: true,
           metadata: %{}
         }}

      :map ->
        {:ok,
         %{
           message_id: "mapped-message",
           conversation_id: "mapped-conversation",
           sender: "mapped-sender",
           content: %{type: :text, text: "mapped"},
           authenticated?: true,
           metadata: %{}
         }}

      :ignore ->
        :ignore

      :error ->
        {:error, :decode_rejected}

      :invalid ->
        {:ok, %{message_id: nil, conversation_id: nil, content: nil}}

      :raise ->
        raise "decode failed"

      :throw ->
        throw(:decode_failed)
    end
  end

  def deliver(outbound, opts) do
    if pid = Keyword.get(opts, :test_pid), do: send(pid, {:runtime_deliver, outbound})
    deliver_reply(Keyword.get(opts, :deliver, :receipt), outbound)
  end

  defp deliver_reply(reply, outbound) do
    case reply do
      :receipt -> {:ok, Receipt.accepted(outbound)}
      :map -> {:ok, %{status: :sent, metadata: %{provider: :map}}}
      :error -> {:error, :delivery_rejected}
      :invalid -> :invalid_reply
      :raise -> raise "delivery failed"
      :throw -> throw(:delivery_failed)
      {:error, _reason} = error -> error
      other -> other
    end
  end

  def subscribe(opts), do: lifecycle(:subscribe, opts)
  def unsubscribe(opts), do: lifecycle(:unsubscribe, opts)

  defp lifecycle(callback, opts) do
    case Keyword.get(opts, callback, :ok) do
      :raise -> raise "lifecycle failed"
      :throw -> throw(:lifecycle_failed)
      reply -> reply
    end
  end
end

defmodule Spectre.Beam.BoundaryContractTest.DecodeOnlyAdapter do
  @moduledoc false

  def decode(_event, _opts), do: :ignore
end

defmodule Spectre.Beam.BoundaryContractTest.CapabilityAdapter do
  @moduledoc false

  def capabilities(opts), do: Keyword.fetch!(opts, :reply)
end

defmodule Spectre.Beam.BoundaryContractTest.ControlledStore do
  @moduledoc false

  def claim(key, opts) do
    notify(opts, {:store_claim, key})
    controlled_reply(:claim, opts, :ok)
  end

  def complete(key, value, opts) do
    notify(opts, {:store_complete, key, value})
    controlled_reply(:complete, opts, :ok)
  end

  def release(key, opts) do
    notify(opts, {:store_release, key})
    controlled_reply(:release, opts, :ok)
  end

  defp controlled_reply(key, opts, default) do
    case Keyword.get(opts, key, default) do
      :raise -> raise "store failed"
      :throw -> throw(:store_failed)
      reply -> reply
    end
  end

  defp notify(opts, message) do
    if pid = Keyword.get(opts, :test_pid), do: send(pid, message)
  end
end

defmodule Spectre.Beam.BoundaryContractTest.InvalidStore do
  @moduledoc false
end

defmodule Spectre.Beam.BoundaryContractTest.TargetResolver3 do
  @moduledoc false

  def resolve(target, endpoint, ctx) do
    case target do
      :raise -> raise "target failed"
      :throw -> throw(:target_failed)
      :reject -> {:error, :target_rejected}
      _other -> {:ok, {endpoint.id, target, ctx.agent}}
    end
  end
end

defmodule Spectre.Beam.BoundaryContractTest.TargetResolver2 do
  @moduledoc false

  def resolve(target, endpoint), do: {:ok, {endpoint.id, target}}
end

defmodule Spectre.Beam.BoundaryContractTest.AgentValues do
  @moduledoc false

  def text_from_input(input, _ctx), do: {:ok, String.upcase(input.text)}
  def document_from_context(ctx), do: %{source: {:memory, ctx.input.text}}
  def rejected_value(_ctx), do: {:error, :value_rejected}
  def raw_value(_ctx), do: "raw-value"
  def raises(_ctx), do: raise("resolver failed")
  def throws(_ctx), do: throw(:resolver_failed)
end

defmodule Spectre.Beam.BoundaryContractTest.TransformPlug do
  @moduledoc false

  alias Spectre.Beam.Pipeline

  def init(opts), do: Keyword.fetch!(opts, :tag)

  def call(pipeline, tag) do
    pipeline
    |> Pipeline.assign(:tag, tag)
    |> Pipeline.put_value({tag, pipeline.value})
  end
end

defmodule Spectre.Beam.BoundaryContractTest.HaltPlug do
  @moduledoc false

  alias Spectre.Beam.Pipeline

  def call(pipeline, opts), do: Pipeline.halt(pipeline, Keyword.get(opts, :result, :ignore))
end

defmodule Spectre.Beam.BoundaryContractTest.RejectPlug do
  @moduledoc false

  def call(_pipeline, opts), do: {:error, Keyword.get(opts, :reason, :rejected)}
end

defmodule Spectre.Beam.BoundaryContractTest.InvalidPlug do
  @moduledoc false

  def call(_pipeline, _opts), do: :invalid
end

defmodule Spectre.Beam.BoundaryContractTest.RaisingPlug do
  @moduledoc false

  def call(_pipeline, :throw), do: throw(:plug_failed)
  def call(_pipeline, _opts), do: raise("plug failed")
end

defmodule Spectre.Beam.BoundaryContractTest.NoCallPlug do
  @moduledoc false
end

defmodule Spectre.Beam.BoundaryContractTest.InboundIdentityPlug do
  @moduledoc false

  alias Spectre.Beam.Pipeline

  def call(%Pipeline{value: inbound} = pipeline, opts) do
    Pipeline.put_value(pipeline, Map.put(inbound, Keyword.fetch!(opts, :field), :changed))
  end
end

defmodule Spectre.Beam.BoundaryContractTest.OutboundIdentityPlug do
  @moduledoc false

  alias Spectre.Beam.Pipeline

  def call(%Pipeline{value: outbound} = pipeline, _opts) do
    Pipeline.put_value(pipeline, %{outbound | idempotency_key: "changed"})
  end
end

defmodule Spectre.Beam.BoundaryContractTest.ReceiptIdentityPlug do
  @moduledoc false

  alias Spectre.Beam.Pipeline

  def call(%Pipeline{value: receipt} = pipeline, _opts) do
    Pipeline.put_value(pipeline, %{receipt | outbound_id: "changed"})
  end
end

defmodule Spectre.Beam.BoundaryContractTest.Agent do
  @moduledoc false

  use Spectre.Agent
  use Spectre.Beam

  beaming do
    channel(:edge,
      type: :external,
      adapter: Spectre.Beam.BoundaryContractTest.RuntimeAdapter,
      capabilities: [:text, :document, :location, :contact, :event],
      planner_exposure: :all
    )

    channel(:decode_only,
      adapter: Spectre.Beam.BoundaryContractTest.DecodeOnlyAdapter
    )

    channel(:wrong_endpoint,
      adapter: Spectre.Beam.BoundaryContractTest.RuntimeAdapter,
      capabilities: [:text],
      inbound_pipeline: [
        {Spectre.Beam.BoundaryContractTest.InboundIdentityPlug, field: :endpoint}
      ]
    )

    channel(:wrong_type,
      type: :external,
      adapter: Spectre.Beam.BoundaryContractTest.RuntimeAdapter,
      capabilities: [:text],
      inbound_pipeline: [
        {Spectre.Beam.BoundaryContractTest.InboundIdentityPlug, field: :channel_type}
      ]
    )
  end
end

defmodule Spectre.Beam.BoundaryContractTest do
  use ExUnit.Case, async: false

  alias Spectre.Action
  alias Spectre.Beam.ActionProvider
  alias Spectre.Beam.Adapters.Common
  alias Spectre.Beam.Adapters.ExGram
  alias Spectre.Beam.Adapters.ExWapp
  alias Spectre.Beam.BoundaryContractTest.Agent
  alias Spectre.Beam.BoundaryContractTest.AgentValues
  alias Spectre.Beam.BoundaryContractTest.CapabilityAdapter
  alias Spectre.Beam.BoundaryContractTest.ControlledStore
  alias Spectre.Beam.BoundaryContractTest.DecodeOnlyAdapter
  alias Spectre.Beam.BoundaryContractTest.HaltPlug
  alias Spectre.Beam.BoundaryContractTest.InvalidPlug
  alias Spectre.Beam.BoundaryContractTest.InvalidStore
  alias Spectre.Beam.BoundaryContractTest.NoCallPlug
  alias Spectre.Beam.BoundaryContractTest.OutboundIdentityPlug
  alias Spectre.Beam.BoundaryContractTest.Provider
  alias Spectre.Beam.BoundaryContractTest.RaisingPlug
  alias Spectre.Beam.BoundaryContractTest.ReceiptIdentityPlug
  alias Spectre.Beam.BoundaryContractTest.RejectPlug
  alias Spectre.Beam.BoundaryContractTest.RuntimeAdapter
  alias Spectre.Beam.BoundaryContractTest.TargetResolver2
  alias Spectre.Beam.BoundaryContractTest.TargetResolver3
  alias Spectre.Beam.BoundaryContractTest.TransformPlug
  alias Spectre.Beam.Config
  alias Spectre.Beam.Content
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Extension
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Pipeline
  alias Spectre.Beam.Receipt
  alias Spectre.Beam.Runtime
  alias Spectre.Beam.Store
  alias Spectre.Context
  alias Spectre.Input
  alias Spectre.State
  alias Spectre.Turn

  setup do
    :ok = Store.reset()
    :ok
  end

  test "provider-neutral values validate portable boundary data" do
    assert %Content{type: :text, text: "hello"} = Content.text("hello")

    assert %Content{type: :document, data: %{id: 1}} =
             Content.new(type: :document, data: %{id: 1})

    assert Content.modalities(Content.text("hello")) == [:text]
    assert Content.modalities(Content.new(type: :image)) == [:image]

    for attrs <- [
          %{type: nil},
          %{type: :text, text: 42},
          %{type: :text, metadata: []}
        ] do
      assert_raise ArgumentError, fn -> Content.new(attrs) end
    end

    inbound =
      Inbound.new(
        message_id: "message-1",
        conversation_id: "conversation-1",
        content: %{type: :text, text: "hello"},
        authenticated?: true
      )

    assert Inbound.new(inbound) == inbound
    assert Inbound.key(inbound) == {nil, "message-1"}
    assert Inbound.conversation_key(inbound) == {:beam, nil, "conversation-1"}

    invalid_inbounds = [
      %{message_id: "", conversation_id: "conversation", content: Content.text("x")},
      %{message_id: "id", conversation_id: nil, content: Content.text("x")},
      %{message_id: "id", conversation_id: "conversation", content: nil},
      %{
        message_id: "id",
        conversation_id: "conversation",
        content: Content.text("x"),
        authenticated?: :yes
      },
      %{
        message_id: "id",
        conversation_id: "conversation",
        content: Content.text("x"),
        metadata: []
      }
    ]

    Enum.each(invalid_inbounds, fn attrs ->
      assert_raise ArgumentError, fn -> Inbound.new(attrs) end
    end)

    outbound =
      Outbound.new(
        endpoint: :edge,
        to: "recipient",
        content: %{type: :text, text: "hello"},
        idempotency_key: "delivery-1"
      )

    assert Outbound.new(outbound) == outbound

    invalid_outbounds = [
      %{endpoint: nil, to: "to", content: Content.text("x"), idempotency_key: "id"},
      %{endpoint: :edge, to: nil, content: Content.text("x"), idempotency_key: "id"},
      %{endpoint: :edge, to: "to", content: nil, idempotency_key: "id"},
      %{endpoint: :edge, to: "to", content: Content.text("x"), idempotency_key: ""},
      %{
        endpoint: :edge,
        to: "to",
        content: Content.text("x"),
        idempotency_key: "id",
        metadata: []
      }
    ]

    Enum.each(invalid_outbounds, fn attrs ->
      assert_raise ArgumentError, fn -> Outbound.new(attrs) end
    end)

    assert %Receipt{status: :accepted} = Receipt.accepted(outbound)
    assert %Receipt{status: :sent} = Receipt.new(status: :sent)
    assert %Receipt{status: :read} = Receipt.new(%Receipt{status: :read})
    assert_raise ArgumentError, fn -> Receipt.new(status: :unknown) end
    assert_raise ArgumentError, fn -> Receipt.new(status: :sent, metadata: []) end
  end

  test "endpoint configuration validates capabilities, pipelines, and planner exposure" do
    explicit =
      Endpoint.new(:explicit,
        adapter: RuntimeAdapter,
        capabilities: MapSet.new([:text]),
        planner_exposure: [:send_text],
        pipelines: %{before_decode: [TransformPlug]},
        send_opts: [parse_mode: :markdown]
      )

    assert Endpoint.capabilities(explicit) == {:ok, MapSet.new([:text])}
    assert Endpoint.pipeline(explicit, :before_decode) == [TransformPlug]

    assert Endpoint.adapter_opts(explicit, adapter_opts: [timeout: 10]) ==
             [send_opts: [parse_mode: :markdown], timeout: 10]

    list_reply = Endpoint.new(:list, adapter: CapabilityAdapter, reply: [:text, :document])
    assert {:ok, capabilities} = Endpoint.capabilities(list_reply)
    assert capabilities == MapSet.new([:text, :document])

    set_reply =
      Endpoint.new(:set,
        adapter: CapabilityAdapter,
        reply: MapSet.new([:location])
      )

    assert Endpoint.capabilities(set_reply) == {:ok, MapSet.new([:location])}

    invalid_reply = Endpoint.new(:invalid_reply, adapter: CapabilityAdapter, reply: :invalid)

    assert {:error, {:invalid_beam_capabilities, :invalid_reply, :invalid}} =
             Endpoint.capabilities(invalid_reply)

    default = Endpoint.new(:default, adapter: DecodeOnlyAdapter)
    assert Endpoint.capabilities(default) == {:ok, MapSet.new([:text])}

    missing = Endpoint.new(:missing, adapter: Spectre.Beam.MissingAdapter)

    assert {:error, {:beam_adapter_not_loaded, :missing, Spectre.Beam.MissingAdapter}} =
             Endpoint.capabilities(missing)

    raising = Endpoint.new(:raising, adapter: RuntimeAdapter, capability_reply: :raise)

    assert {:error, {:beam_capabilities_exception, :raising, RuntimeError}} =
             Endpoint.capabilities(raising)

    throwing = Endpoint.new(:throwing, adapter: RuntimeAdapter, capability_reply: :throw)

    assert {:error, {:beam_capabilities_failure, :throwing, :throw, :capabilities_failed}} =
             Endpoint.capabilities(throwing)

    invalid_endpoint_options = [
      fn -> Endpoint.new(nil, adapter: RuntimeAdapter) end,
      fn -> Endpoint.new("", adapter: RuntimeAdapter) end,
      fn -> Endpoint.new(:edge, adapter: "invalid") end,
      fn -> Endpoint.new(:edge, adapter: RuntimeAdapter, capabilities: :invalid) end,
      fn -> Endpoint.new(:edge, adapter: RuntimeAdapter, planner_exposure: [:send_text, 1]) end,
      fn -> Endpoint.new(:edge, adapter: RuntimeAdapter, pipelines: :invalid) end,
      fn -> Endpoint.new(:edge, adapter: RuntimeAdapter, pipelines: [:not_a_keyword]) end,
      fn -> Endpoint.new(:edge, adapter: RuntimeAdapter, pipelines: [unknown: []]) end
    ]

    Enum.each(invalid_endpoint_options, &assert_raise(ArgumentError, &1))
  end

  test "pipelines transform, halt, and fail closed without changing boundary identity" do
    endpoint = Endpoint.new(:edge, adapter: RuntimeAdapter)

    assert {:ok, {:trusted, :event}, pipeline} =
             Pipeline.run(
               :before_decode,
               endpoint,
               :event,
               [{TransformPlug, tag: :trusted}]
             )

    assert pipeline.assigns == %{tag: :trusted}
    assert pipeline.private.runtime_opts == []

    assert {:halt, :ignore, halted} =
             Pipeline.run(:before_decode, endpoint, :event, [HaltPlug])

    assert halted.halted?

    assert {:halt, :stop, _pipeline} =
             Pipeline.run(:before_decode, endpoint, :event, [{HaltPlug, result: :stop}])

    assert {:error, {:beam_pipeline_rejected, :before_decode, RejectPlug, :policy}} =
             Pipeline.run(
               :before_decode,
               endpoint,
               :event,
               [{RejectPlug, reason: :policy}]
             )

    assert {:error, {:invalid_beam_plug_reply, :before_decode, InvalidPlug, :invalid}} =
             Pipeline.run(:before_decode, endpoint, :event, [InvalidPlug])

    assert {:error,
            {:beam_pipeline_rejected, :before_decode, NoCallPlug,
             {:invalid_beam_plug, :before_decode, NoCallPlug}}} =
             Pipeline.run(:before_decode, endpoint, :event, [NoCallPlug])

    assert {:error,
            {:beam_pipeline_rejected, :before_decode, Spectre.Beam.MissingPlug,
             {:beam_plug_not_loaded, :before_decode, Spectre.Beam.MissingPlug}}} =
             Pipeline.run(:before_decode, endpoint, :event, [Spectre.Beam.MissingPlug])

    assert {:error, {:beam_plug_exception, :before_decode, RaisingPlug, RuntimeError}} =
             Pipeline.run(:before_decode, endpoint, :event, [RaisingPlug])

    assert {:error, {:beam_plug_failure, :before_decode, RaisingPlug, :throw, :plug_failed}} =
             Pipeline.run(:before_decode, endpoint, :event, [{RaisingPlug, :throw}])

    assert_raise ArgumentError, fn ->
      Pipeline.validate_specs!([{TransformPlug, [:not_a_keyword]}], :before_decode)
    end

    assert_raise ArgumentError, fn ->
      Pipeline.validate_specs!([{"invalid", []}], :before_decode)
    end

    assert_raise ArgumentError, fn ->
      Pipeline.validate_specs!(:invalid, :before_decode)
    end
  end

  test "adapter helpers normalize provider data without leaking runtime options" do
    unix_seconds = 1_774_958_400
    now = DateTime.from_unix!(unix_seconds)
    naive = DateTime.to_naive(now)

    assert Common.get(%{key: 1}, :key) == 1
    assert Common.get(%{"key" => 2}, :key) == 2
    assert Common.get(:invalid, :key, :default) == :default
    assert Common.first(%{second: "value"}, [:first, :second]) == "value"
    assert Common.id("id") == {:ok, "id"}
    assert Common.id(42) == {:ok, "42"}
    assert Common.id(:atom) == {:ok, "atom"}
    assert Common.id(nil) == {:error, :missing_provider_message_id}
    assert Common.occurred_at(now) == now
    assert Common.occurred_at(naive) == now
    assert Common.occurred_at(unix_seconds) == now
    assert DateTime.compare(Common.occurred_at(unix_seconds * 1_000), now) == :eq
    assert Common.occurred_at(99_999_999_999_999_999_999) == nil
    assert Common.occurred_at(:invalid) == nil
    assert Common.authenticated?([]) == true
    assert Common.authenticated?(authenticated?: false) == false
    assert Common.client(client: :client) == {:ok, :client}
    assert Common.client(session: :session) == {:ok, :session}
    assert Common.client([]) == {:error, :missing_beam_adapter_client}
    assert Common.provider_module([], Provider) == {:ok, Provider}

    assert Common.provider_module([module: "invalid"], Provider) ==
             {:error, {:invalid_beam_provider_module, "invalid"}}

    assert Common.call(Spectre.Beam.MissingProvider, :send, []) ==
             {:error, {:beam_provider_not_loaded, Spectre.Beam.MissingProvider}}

    assert Common.call(Provider, :missing, []) ==
             {:error, {:beam_provider_callback_missing, Provider, :missing, 0}}

    assert Common.content(:text, nil, "hello", %{provider: :test}) ==
             {:ok, Content.text("hello", metadata: %{provider: :test})}

    assert {:ok, %Content{type: :image}} =
             Common.content(:media, %{type: :photo}, nil)

    assert {:ok, %Content{type: :video}} =
             Common.content(:media, %{media_type: :video}, nil)

    assert {:ok, %Content{type: :audio}} =
             Common.content(:media, %{type: :voice_note}, nil)

    assert {:ok, %Content{type: :document}} =
             Common.content(:media, %{type: :unknown}, nil)

    assert {:ok, %Content{type: :image}} = Common.content(:message_photo, %{}, nil)
    assert {:ok, %Content{type: :document}} = Common.content(:message_document, %{}, nil)
    assert {:ok, %Content{type: :audio}} = Common.content(:voice_note, %{}, nil)
    assert {:ok, %Content{type: :text}} = Common.content(:unknown, nil, "fallback")
    assert Common.content(:unknown, nil, nil) == :ignore
    assert Common.content(nil, nil, "fallback") == {:ok, Content.text("fallback")}
    assert Common.content(nil, nil, nil) == :ignore
    assert Common.source(%{source: "source"}) == "source"
    assert Common.source(%{"document" => "document"}) == "document"
    assert Common.source("source") == "source"
    assert Common.data_value(%{"latitude" => 1}, :latitude) == 1

    outbound =
      Outbound.new(
        endpoint: :edge,
        to: :target,
        reply_to: :reply,
        content: Content.new(type: :document, data: %{opts: [caption: "data"]}),
        idempotency_key: "common-delivery"
      )

    send_options =
      Common.send_options(outbound, send_opts: [caption: "configured", silent: true])

    assert send_options[:caption] == "data"
    assert send_options[:silent]
    assert send_options[:reply_to] == :reply

    assert Common.provider_options(
             client: self(),
             timeout: 10,
             authenticated?: false,
             custom: :kept
           ) == [custom: :kept]

    assert {:ok, %Receipt{metadata: %{dispatch: :asynchronous}}} =
             Common.normalize_delivery(:ok, outbound)

    assert {:ok, %Receipt{provider_message_id: "provider"}} =
             Common.normalize_delivery({:ok, "provider"}, outbound)

    assert {:ok, %Receipt{provider_message_id: "provider"}} =
             Common.normalize_delivery({:ok, :client, "provider"}, outbound)

    assert Common.normalize_delivery({:error, :failed}, outbound) == {:error, :failed}
    assert Common.normalize_delivery({:error, :failed, :client}, outbound) == {:error, :failed}

    assert Common.normalize_delivery(:invalid, outbound) ==
             {:error, {:invalid_beam_provider_reply, :invalid}}

    assert Common.normalize_lifecycle_reply(:ok) == :ok
    assert Common.normalize_lifecycle_reply({:ok, :value}) == :ok
    assert Common.normalize_lifecycle_reply({:error, :failed}) == {:error, :failed}

    assert Common.normalize_lifecycle_reply(:invalid) ==
             {:error, {:invalid_beam_provider_lifecycle_reply, :invalid}}
  end

  test "ExGram decodes every supported envelope and preserves provenance" do
    base = %{
      id: 101,
      chat_id: -100,
      sender_id: 42,
      from_me: false,
      timestamp: 1_774_958_400
    }

    cases = [
      {%{kind: :text, text: "hello"}, :text},
      {%{content: %{"@type": "messagePhoto", caption: "photo", media: %{id: "p"}}}, :image},
      {%{content: %{"@type": "messageVideo", media: %{id: "v"}}}, :video},
      {%{content: %{"@type": "messageAnimation", media: %{id: "a"}}}, :image},
      {%{content: %{"@type": "messageDocument", media: %{id: "d"}}}, :document},
      {%{content: %{"@type": "messageAudio", media: %{id: "a"}}}, :audio},
      {%{content: %{"@type": "messageVoiceNote", media: %{id: "v"}}}, :audio},
      {%{content: %{"@type": "messageLocation", location: %{latitude: 1}}}, :location},
      {%{content: %{"@type": "messageVenue", location: %{latitude: 1}}}, :location},
      {%{content: %{"@type": "messageContact", contact: %{name: "A"}}}, :contact},
      {%{location: %{latitude: 1}}, :location},
      {%{contact: %{name: "A"}}, :contact},
      {%{document: %{id: "doc"}}, :document}
    ]

    Enum.each(cases, fn {extra, type} ->
      message = Map.merge(base, extra)
      assert {:ok, inbound} = ExGram.decode(message, recipient: :agent, authenticated?: false)
      assert inbound.content.type == type
      assert inbound.recipient == :agent
      refute inbound.authenticated?
      assert inbound.metadata.provider == :ex_gram
    end)

    nested = %{message: Map.put(base, :text, "nested")}
    assert {:ok, %Inbound{content: %Content{text: "nested"}}} = ExGram.decode(nested, [])

    assert {:ok, session_inbound} =
             ExGram.decode(
               {:ex_gram_message, :session, "jid", Map.put(base, :text, "tuple")},
               []
             )

    assert session_inbound.metadata.session == :session
    assert session_inbound.conversation_id == "jid"

    assert {:ok, no_session} =
             ExGram.decode({:ex_gram_message, "jid", Map.put(base, :text, "tuple")}, [])

    refute Map.has_key?(no_session.metadata, :session)
    assert ExGram.decode(Map.merge(base, %{from_me: true, text: "ignored"}), []) == :ignore
    assert ExGram.decode(Map.merge(base, %{id: nil, text: "ignored"}), []) == :ignore
    assert ExGram.decode(%{id: 1, kind: :unknown}, []) == :ignore
    assert ExGram.decode(:invalid, []) == :ignore

    assert ExGram.decode(%{id: 1, kind: :text, text: "missing conversation"}, []) ==
             {:error, :missing_ex_gram_conversation}
  end

  test "ExGram delivery covers typed content and dynamic provider failures" do
    delivery_cases = [
      {Content.text("hello"), {:provider_call, :text, :to, "hello"}},
      {
        Content.new(type: :document, data: %{source: "document"}),
        {:provider_call, :document, :to, "document", [reply_to: :reply]}
      },
      {
        Content.new(type: :location, data: %{latitude: 1, longitude: 2}),
        {:provider_call, :location, :to, 1, 2, [reply_to: :reply]}
      },
      {
        Content.new(type: :contact, data: %{display_name: "A", vcard: "VCARD"}),
        {:provider_call, :contact, :to, "A", "VCARD", [reply_to: :reply]}
      },
      {
        Content.new(type: :event, data: %{name: "Event", start_time: "now"}),
        {:provider_call, :event, :to, "Event", "now", [reply_to: :reply]}
      }
    ]

    Enum.each(delivery_cases, fn {content, expected_call} ->
      outbound =
        Outbound.new(
          endpoint: :telegram,
          to: :to,
          reply_to: :reply,
          content: content,
          idempotency_key: "gram-#{content.type}"
        )

      assert {:ok, %Receipt{status: :accepted}} =
               ExGram.deliver(outbound, module: Provider, client: self())

      assert_receive ^expected_call
    end)

    unsupported =
      Outbound.new(
        endpoint: :telegram,
        to: :to,
        content: Content.new(type: :unsupported),
        idempotency_key: "gram-unsupported"
      )

    assert ExGram.deliver(unsupported, module: Provider, client: self()) ==
             {:error, {:unsupported_ex_gram_content, :unsupported}}

    assert ExGram.deliver(unsupported, module: Provider) ==
             {:error, :missing_beam_adapter_client}

    assert ExGram.deliver(unsupported, module: "invalid", client: self()) ==
             {:error, {:invalid_beam_provider_module, "invalid"}}

    assert :ok = ExGram.subscribe(module: Provider, client: self())
    assert_receive :provider_subscribed
    assert :ok = ExGram.unsubscribe(module: Provider, client: self())
    assert_receive :provider_unsubscribed
  end

  test "ExWapp decodes event shapes and delivers acknowledged typed content" do
    base = %{
      id: "message-1",
      jid: "chat",
      participant: "sender",
      from_me: false,
      timestamp: 1_774_958_400
    }

    cases = [
      {%{text: "hello"}, :text},
      {%{media: %{type: :photo, id: "photo"}}, :image},
      {%{location: %{latitude: 1}}, :location},
      {%{contact: %{name: "A"}}, :contact},
      {%{event: %{name: "Event"}}, :event},
      {%{content: %{kind: :protocol, text: "protocol"}}, :text}
    ]

    Enum.each(cases, fn {extra, type} ->
      assert {:ok, inbound} = ExWapp.decode(Map.merge(base, extra), [])
      assert inbound.content.type == type
      assert inbound.conversation_id == "chat"
    end)

    assert {:ok, with_session} =
             ExWapp.decode(
               {:ex_wapp_message, :session, "jid", Map.put(base, :text, "tuple")},
               []
             )

    assert with_session.metadata.session == :session

    assert {:ok, without_session} =
             ExWapp.decode({:ex_wapp_message, "jid", Map.put(base, :text, "tuple")}, [])

    refute Map.has_key?(without_session.metadata, :session)
    assert ExWapp.decode(Map.merge(base, %{from_me: true, text: "ignored"}), []) == :ignore
    assert ExWapp.decode(Map.merge(base, %{id: nil, text: "ignored"}), []) == :ignore
    assert ExWapp.decode(%{id: "id", text: "missing jid"}, []) == :ignore
    assert ExWapp.decode(:invalid, []) == :ignore

    delivery_cases = [
      {Content.text("hello"), {:provider_call, :text, :to, "hello"}},
      {
        Content.new(type: :document, data: %{source: "document"}),
        {:provider_call, :document, :to, "document", []}
      },
      {
        Content.new(type: :location, data: %{latitude: 1, longitude: 2}),
        {:provider_call, :location, :to, 1, 2, []}
      },
      {
        Content.new(type: :contact, data: %{display_name: "A", vcard: "VCARD"}),
        {:provider_call, :contact, :to, "A", "VCARD", []}
      },
      {
        Content.new(type: :event, data: %{name: "Event", start_time: "now"}),
        {:provider_call, :event, :to, "Event", "now", []}
      }
    ]

    Enum.each(delivery_cases, fn {content, expected_call} ->
      outbound =
        Outbound.new(
          endpoint: :whatsapp,
          to: :to,
          content: content,
          idempotency_key: "wapp-#{content.type}"
        )

      assert {:ok, %Receipt{status: :accepted}} =
               ExWapp.deliver(outbound, module: Provider, client: self())

      assert_receive ^expected_call
    end)

    text =
      Outbound.new(
        endpoint: :whatsapp,
        to: :to,
        content: Content.text("hello"),
        idempotency_key: "wapp-ack"
      )

    assert {:ok, %Receipt{metadata: %{dispatch: :acknowledged}}} =
             ExWapp.deliver(text,
               module: Provider,
               client: self(),
               await_ack: true,
               timeout: 7
             )

    assert_receive {:provider_call, :await_text, :to, "hello", 7}

    timeout = %{text | content: Content.text("timeout")}

    assert {:error, {:ambiguous, :ack_timeout}} =
             ExWapp.deliver(timeout,
               module: Provider,
               client: self(),
               await_ack: true
             )

    assert {:error, {:unsupported_ex_wapp_content, :unsupported}} =
             ExWapp.deliver(
               %{text | content: Content.new(type: :unsupported)},
               module: Provider,
               client: self()
             )
  end

  test "ActionProvider exposes closed operations and executes only with idempotency" do
    endpoint =
      Endpoint.new(:edge,
        type: :external,
        adapter: RuntimeAdapter,
        capabilities: [:text, :document, :unknown],
        planner_exposure: [:send_text],
        target_resolver: TargetResolver3
      )

    assert [document, text] = ActionProvider.actions(endpoint: endpoint)
    assert document.name == :send_document
    assert document.visibility == :deterministic
    assert text.name == :send_text
    assert text.visibility == :both

    text_action =
      Action.new(:send_text,
        via: {:beam, :edge},
        args: %{"to" => "recipient", text: :text_from_input}
      )

    assert ActionProvider.schema_hash(text_action, endpoint: endpoint) == text.schema_hash
    assert ActionProvider.schema_hash(%{text_action | name: :missing}, endpoint: endpoint) == nil

    context = %Context{
      agent: AgentValues,
      input: Input.new("hello"),
      state: %State{},
      opts: [
        idempotency_key: "action-delivery",
        adapter_opts: [test_pid: self()]
      ]
    }

    assert {:ok, %Receipt{status: :accepted}} =
             ActionProvider.execute(text_action, context, endpoint: endpoint)

    assert_receive {:runtime_deliver, outbound}
    assert outbound.to == {:edge, "recipient", AgentValues}
    assert outbound.content.text == "HELLO"
    assert outbound.metadata.kind == :proactive

    assert {:error, :missing_beam_idempotency_key} =
             ActionProvider.execute(text_action, %{context | opts: []}, endpoint: endpoint)

    assert {:error, {:beam_action_provider_mismatch, {:beam, :other}, :edge}} =
             ActionProvider.execute(
               %{text_action | via: {:beam, :other}},
               context,
               endpoint: endpoint
             )

    assert {:error, {:unsupported_beam_operation, :edge, :unsupported}} =
             ActionProvider.execute(
               %{text_action | name: :unsupported},
               context,
               endpoint: endpoint
             )

    assert {:error, {:missing_beam_target, :edge}} =
             ActionProvider.execute(
               %{text_action | args: %{text: "hello"}},
               context,
               endpoint: endpoint
             )

    assert {:error, :invalid_beam_text} =
             ActionProvider.execute(
               %{text_action | args: %{to: "to", text: " "}},
               context,
               endpoint: %{endpoint | target_resolver: nil}
             )

    assert {:error, :value_rejected} =
             ActionProvider.execute(
               %{text_action | args: %{to: "to", text: :rejected_value}},
               context,
               endpoint: %{endpoint | target_resolver: nil}
             )

    assert {:error, {:beam_value_resolver_exception, :raises, RuntimeError}} =
             ActionProvider.execute(
               %{text_action | args: %{to: "to", text: :raises}},
               context,
               endpoint: %{endpoint | target_resolver: nil}
             )

    assert {:error, {:beam_value_resolver_failure, :throws, :throw, :resolver_failed}} =
             ActionProvider.execute(
               %{text_action | args: %{to: "to", text: :throws}},
               context,
               endpoint: %{endpoint | target_resolver: nil}
             )

    document_action =
      Action.new(:send_document,
        via: {:beam, :edge},
        args: %{to: "to", document: :document_from_context}
      )

    assert {:ok, %Receipt{}} =
             ActionProvider.execute(
               document_action,
               context,
               endpoint: %{
                 endpoint
                 | target_resolver: TargetResolver2,
                   capabilities: MapSet.new([:document])
               }
             )

    assert {:error, {:missing_beam_content, :document}} =
             ActionProvider.execute(
               %{document_action | args: %{to: "to", document: nil}},
               context,
               endpoint: %{endpoint | target_resolver: nil}
             )

    resolver_fun = fn target, resolved_endpoint, _ctx ->
      {:ok, {resolved_endpoint.id, target, :fun}}
    end

    assert {:ok, %Receipt{}} =
             ActionProvider.execute(
               text_action,
               context,
               endpoint: %{endpoint | target_resolver: resolver_fun}
             )

    assert {:error, {:invalid_beam_target_resolver, :edge, :invalid}} =
             ActionProvider.execute(
               text_action,
               context,
               endpoint: %{endpoint | target_resolver: :invalid}
             )

    assert {:error, {:beam_target_resolver_exception, :edge, RuntimeError}} =
             ActionProvider.execute(
               %{text_action | args: %{to: :raise, text: "hello"}},
               context,
               endpoint: endpoint
             )

    assert {:error, {:beam_target_resolver_failure, :edge, :throw, :target_failed}} =
             ActionProvider.execute(
               %{text_action | args: %{to: :throw, text: "hello"}},
               context,
               endpoint: endpoint
             )
  end

  test "runtime decode and lifecycle failures remain provider-neutral" do
    assert {:ok, inbound} =
             Spectre.Beam.decode(
               Agent,
               :edge,
               %{id: "mapped"},
               adapter_opts: [decode: :map]
             )

    assert inbound.endpoint == :edge
    assert inbound.channel_type == :external
    assert inbound.message_id == "mapped-message"

    input = Spectre.Beam.to_input(inbound)
    assert input.source.kind == :beam
    assert input.source.mount == :edge
    assert input.source.metadata.modalities == [:text]

    assert Spectre.Beam.decode(Agent, :edge, %{}, adapter_opts: [decode: :ignore]) == :ignore

    assert Spectre.Beam.decode(Agent, :edge, %{}, adapter_opts: [decode: :error]) ==
             {:error, :decode_rejected}

    assert {:error, {:invalid_beam_inbound, :edge, _message}} =
             Spectre.Beam.decode(Agent, :edge, %{}, adapter_opts: [decode: :invalid])

    assert Spectre.Beam.decode(Agent, :edge, %{}, adapter_opts: [decode: :raise]) ==
             {:error, {:beam_decode_exception, :edge, RuntimeError}}

    assert Spectre.Beam.decode(Agent, :edge, %{}, adapter_opts: [decode: :throw]) ==
             {:error, {:beam_decode_failure, :edge, :throw, :decode_failed}}

    assert {:error, {:beam_payload_too_large, :edge, bytes, 1}} =
             Spectre.Beam.decode(Agent, :edge, %{large: String.duplicate("x", 20)},
               max_payload_bytes: 1
             )

    assert bytes > 1

    assert Spectre.Beam.decode(Agent, :missing, %{}) ==
             {:error, {:unknown_beam_endpoint, :missing}}

    assert Spectre.Beam.decode(Agent, :decode_only, %{}) == :ignore

    assert :ok = Spectre.Beam.subscribe(Agent, :edge)
    assert :ok = Spectre.Beam.unsubscribe(Agent, :edge)

    assert Spectre.Beam.subscribe(Agent, :edge, adapter_opts: [subscribe: {:error, :denied}]) ==
             {:error, :denied}

    assert Spectre.Beam.subscribe(Agent, :edge, adapter_opts: [subscribe: :invalid]) ==
             {:error, {:invalid_beam_adapter_lifecycle_reply, :edge, :subscribe, :invalid}}

    assert Spectre.Beam.subscribe(Agent, :edge, adapter_opts: [subscribe: :raise]) ==
             {:error, {:beam_adapter_lifecycle_exception, :edge, :subscribe, RuntimeError}}

    assert Spectre.Beam.subscribe(Agent, :edge, adapter_opts: [subscribe: :throw]) ==
             {:error,
              {:beam_adapter_lifecycle_failure, :edge, :subscribe, :throw, :lifecycle_failed}}

    assert Spectre.Beam.subscribe(Agent, :decode_only) ==
             {:error, {:beam_adapter_lifecycle_not_supported, :decode_only, :subscribe}}
  end

  test "runtime delivery fences duplicates, ambiguous outcomes, and pipeline identity" do
    outbound =
      Outbound.new(
        endpoint: :edge,
        to: "recipient",
        content: Content.text("hello"),
        idempotency_key: "runtime-delivery"
      )

    endpoint =
      Endpoint.new(:edge,
        adapter: RuntimeAdapter,
        capabilities: [:text]
      )

    duplicate = Receipt.accepted(outbound)

    assert Runtime.deliver(endpoint, outbound,
             idempotency_store: {ControlledStore, [claim: {:duplicate, duplicate}]}
           ) == {:ok, duplicate}

    assert Runtime.deliver(endpoint, outbound,
             idempotency_store: {ControlledStore, [claim: :in_progress]}
           ) == {:error, {:beam_delivery_in_progress, "runtime-delivery"}}

    assert Runtime.deliver(endpoint, outbound,
             idempotency_store: {ControlledStore, [claim: {:error, :store_down}]}
           ) == {:error, :store_down}

    assert {:ok, %Receipt{status: :sent, endpoint: :edge, outbound_id: "runtime-delivery"}} =
             Runtime.deliver(endpoint, outbound,
               adapter_opts: [deliver: :map],
               idempotency_store: {ControlledStore, [test_pid: self()]}
             )

    assert_receive {:store_complete, {:outbound, :edge, "runtime-delivery"}, %Receipt{}}

    assert Runtime.deliver(endpoint, outbound,
             adapter_opts: [deliver: :invalid],
             idempotency_store: {ControlledStore, [test_pid: self()]}
           ) == {:error, {:invalid_beam_delivery_reply, :edge, :invalid_reply}}

    assert_receive {:store_release, {:outbound, :edge, "runtime-delivery"}}

    assert Runtime.deliver(endpoint, outbound,
             adapter_opts: [deliver: :raise],
             idempotency_store: {ControlledStore, [test_pid: self()]}
           ) == {:error, {:ambiguous, {:beam_delivery_exception, RuntimeError}}}

    refute_receive {:store_release, {:outbound, :edge, "runtime-delivery"}}

    assert Runtime.deliver(endpoint, outbound,
             adapter_opts: [deliver: :throw],
             idempotency_store: {ControlledStore, []}
           ) == {:error, {:ambiguous, {:beam_delivery_failure, :throw, :delivery_failed}}}

    unsupported = %{outbound | content: Content.new(type: :document)}

    assert Runtime.deliver(endpoint, unsupported, idempotency_store: {ControlledStore, []}) ==
             {:error, {:unsupported_beam_capability, :edge, :document}}

    outbound_identity =
      Endpoint.new(:edge,
        adapter: RuntimeAdapter,
        capabilities: [:text],
        outbound_pipeline: [OutboundIdentityPlug]
      )

    assert {:error, {:invalid_beam_outbound_pipeline_value, :edge, %Outbound{}}} =
             Runtime.deliver(outbound_identity, outbound,
               idempotency_store: {ControlledStore, []}
             )

    receipt_identity =
      Endpoint.new(:edge,
        adapter: RuntimeAdapter,
        capabilities: [:text],
        receipt_pipeline: [ReceiptIdentityPlug]
      )

    assert {:error, {:invalid_beam_receipt_pipeline_value, :edge, %Receipt{}}} =
             Runtime.deliver(receipt_identity, outbound, idempotency_store: {ControlledStore, []})

    ignored =
      Endpoint.new(:edge,
        adapter: RuntimeAdapter,
        capabilities: [:text],
        outbound_pipeline: [HaltPlug]
      )

    assert Runtime.deliver(ignored, outbound, idempotency_store: {ControlledStore, []}) ==
             {:error, {:beam_pipeline_ignored_delivery, :edge}}

    halted =
      Endpoint.new(:edge,
        adapter: RuntimeAdapter,
        capabilities: [:text],
        outbound_pipeline: [{HaltPlug, result: :manual_stop}]
      )

    assert Runtime.deliver(halted, outbound, idempotency_store: {ControlledStore, []}) ==
             {:error, {:beam_pipeline_halted, :edge, :before_deliver, :manual_stop}}

    assert Runtime.deliver(endpoint, outbound, idempotency_store: InvalidStore) ==
             {:error, {:invalid_beam_idempotency_store, InvalidStore, :claim}}

    assert Runtime.deliver(endpoint, outbound,
             idempotency_store: {ControlledStore, [claim: :raise]}
           ) ==
             {:error, {:beam_idempotency_store_exception, ControlledStore, :claim, RuntimeError}}

    assert Runtime.deliver(endpoint, outbound,
             idempotency_store: {ControlledStore, [claim: :throw]}
           ) ==
             {:error,
              {:beam_idempotency_store_failure, ControlledStore, :claim, :throw, :store_failed}}
  end

  test "decode pipelines cannot rewrite endpoint identity or bypass policy" do
    assert {:error, {:invalid_beam_inbound_pipeline_value, :wrong_endpoint, %Inbound{}}} =
             Spectre.Beam.decode(Agent, :wrong_endpoint, %{id: "message"})

    assert {:error, {:invalid_beam_inbound_pipeline_value, :wrong_type, %Inbound{}}} =
             Spectre.Beam.decode(Agent, :wrong_type, %{id: "message"})
  end

  test "Stack extension compilation rejects ambiguous boundary declarations" do
    assert Extension.id() == :beam
    assert Extension.api_version() == 1

    assert {:ok, config} =
             Extension.compile(__MODULE__,
               stack_config: %{
                 channels: [
                   {:one, RuntimeAdapter},
                   {:two, [adapter: RuntimeAdapter, capabilities: [:text]]}
                 ],
                 options: [max_payload_bytes: 100]
               }
             )

    assert %Config{} = config
    assert Enum.map(config.endpoints, & &1.id) == [:one, :two]
    assert Extension.agent_config(config) == [beam: config]
    assert Enum.map(Extension.action_providers(config), & &1.id) == [{:beam, :one}, {:beam, :two}]

    assert {[], [timeout: 10]} = Extension.flow_constraints([timeout: 10], config)

    assert {[constraint], [timeout: 10]} =
             Extension.flow_constraints([beam: [:one, :two], timeout: 10], config)

    assert constraint.namespace == :beam
    assert constraint.values == [:one, :two]

    assert Extension.flow_constraints([beam: []], config) ==
             {:error, :beam_flow_requires_endpoint}

    assert Extension.flow_constraints([beam: :missing], config) ==
             {:error, {:unknown_beam_endpoint, :missing}}

    assert Extension.compile(__MODULE__,
             stack_config: %{channels: [{:one, RuntimeAdapter}, {:one, RuntimeAdapter}]}
           ) == {:error, {:duplicate_beam_channel, :one}}

    assert {:error, {:invalid_beam_endpoint, _message}} =
             Extension.compile(__MODULE__,
               stack_config: %{channels: [{:invalid, [adapter: "invalid"]}]}
             )

    handler =
      quote do
        beam("target", via: :one, document: %{id: 1}, policy: :confirm)
      end

    assert {:ok, _quoted} = Extension.expand_handler(handler, __ENV__, [])

    assert Extension.expand_handler({:other, [], []}, __ENV__, []) == :ignore

    invalid_handler =
      quote do
        beam("target", :invalid)
      end

    assert Extension.expand_handler(invalid_handler, __ENV__, []) ==
             {:error, {:invalid_beam_handler_options, :invalid}}

    assert {:ok, %{channels: []}} = Spectre.Beam.compile([], nil, __ENV__)
  end

  test "public handle rejects invalid sessions and preserves non-reply boundaries" do
    assert Spectre.Beam.handle(:not_a_session, :edge, %{}) ==
             {:error, {:invalid_spectre_session, :not_a_session}}

    inbound =
      Inbound.new(
        endpoint: :edge,
        channel_type: :external,
        message_id: "message",
        conversation_id: "conversation",
        sender: "sender",
        content: Content.text("hello"),
        authenticated?: true
      )

    assert Spectre.Beam.reply(Agent, inbound, %Turn{}, []) == {:ok, nil}

    hidden = %Spectre.Result{
      state: %State{},
      reply_text: nil,
      metadata: %{}
    }

    assert Spectre.Beam.reply(
             Agent,
             inbound,
             %Turn{observable: nil, decision: {:reply, hidden}},
             []
           ) == {:ok, nil}
  end
end
