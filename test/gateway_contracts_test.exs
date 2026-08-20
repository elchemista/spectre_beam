defmodule Spectre.Beam.RefTest do
  use ExUnit.Case, async: true

  alias Spectre.Beam.Content
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Ref

  doctest Spectre.Beam.Ref

  test "an integer and a binary conversation id share one address" do
    from_provider = Ref.new(endpoint: :telegram, conversation_id: 12_345)
    from_terminal = Ref.parse!("telegram:12345")

    assert Ref.slug(from_provider) == Ref.slug(from_terminal)
    assert Ref.key(from_provider) == Ref.key(from_terminal)
  end

  test "keeps the provider's own term so an outbound addresses it unchanged" do
    ref = Ref.new(endpoint: :telegram, conversation_id: 12_345)
    assert ref.conversation_id == 12_345
  end

  test "resolves a parsed endpoint against the known ones" do
    ref = Ref.parse!("telegram:1", endpoints: [:telegram, :whatsapp])
    assert ref.endpoint == :telegram

    unknown = Ref.parse!("signal:1", endpoints: [:telegram])
    assert unknown.endpoint == "signal"
  end

  test "rejects an address without both halves" do
    assert {:error, {:invalid_beam_ref, "telegram"}} = Ref.parse("telegram")
    assert {:error, {:invalid_beam_ref, ":42"}} = Ref.parse(":42")
    assert {:error, {:invalid_beam_ref, 42}} = Ref.parse(42)
  end

  test "builds from a normalized inbound" do
    inbound =
      Inbound.new(%{
        endpoint: :telegram,
        message_id: "m-1",
        conversation_id: 99,
        sender: "sender",
        content: Content.text("hi")
      })

    ref = Ref.from_inbound(inbound, gateway: :gw, agent: SomeAgent, scope: :instance)

    assert ref.gateway == :gw
    assert ref.agent == SomeAgent
    assert ref.scope == :instance
    assert Ref.slug(ref) == "telegram:99"
  end

  test "separates the conversation topic from the endpoint topic" do
    ref = Ref.new(gateway: :gw, endpoint: :telegram, conversation_id: "1")

    assert Ref.topic(ref) == {:conversation, :gw, "telegram:1"}
    assert Ref.endpoint_topic(ref) == {:endpoint, :gw, :telegram}
  end

  test "refuses an invalid scope or endpoint" do
    assert_raise ArgumentError, ~r/scope/, fn ->
      Ref.new(endpoint: :telegram, conversation_id: "1", scope: :nope)
    end

    assert_raise ArgumentError, ~r/endpoint/, fn ->
      Ref.new(endpoint: "", conversation_id: "1")
    end
  end

  test "prints as its address" do
    assert inspect(Ref.new(endpoint: :telegram, conversation_id: 7)) == "#Beam.Ref<telegram:7>"
  end
end

defmodule Spectre.Beam.EventTest do
  use ExUnit.Case, async: true

  alias Spectre.Beam.Event
  alias Spectre.Beam.Ref

  setup do
    %{ref: Ref.new(gateway: :gw, endpoint: :chat, conversation_id: "1")}
  end

  test "refuses a type outside the published set", %{ref: ref} do
    invented = String.to_atom("beam_invented_event_type")

    assert_raise ArgumentError, ~r/unknown Beam event type/, fn ->
      Event.new(invented, ref, %{})
    end
  end

  test "carries the contract version", %{ref: ref} do
    assert Event.new(:reply, ref, %{text: "hi"}).v == Event.version()
  end

  # A transport must never silently lose an event because one field of its
  # payload happened not to be JSON-encodable.
  test "renders every payload as an encodable map", %{ref: ref} do
    event =
      Event.new(:receipt, ref, %{
        status: :accepted,
        at: ~U[2026-08-20 10:00:00Z],
        pid: self(),
        nested: %{count: 2, tuple: {:a, :b}},
        list: [:one, "two"]
      })

    assert %{
             "type" => "receipt",
             "ref" => "chat:1",
             "payload" => payload
           } = Event.to_map(event)

    assert payload["status"] == "accepted"
    assert payload["at"] == "2026-08-20T10:00:00Z"
    assert is_binary(payload["pid"])
    assert payload["nested"]["count"] == 2
    assert is_binary(payload["nested"]["tuple"])
    assert payload["list"] == ["one", "two"]
  end
end

defmodule Spectre.Beam.BusTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Bus
  alias Spectre.Beam.Event
  alias Spectre.Beam.Ref
  alias Spectre.Beam.Sequence

  setup do
    ref =
      Ref.new(
        gateway: :"bus_#{System.unique_integer([:positive])}",
        endpoint: :chat,
        conversation_id: "1"
      )

    on_exit(fn -> Sequence.forget(Ref.topic(ref)) end)
    %{ref: ref, bus: Bus.default()}
  end

  test "delivers on both the conversation and the endpoint topic", %{ref: ref, bus: bus} do
    :ok = Bus.subscribe(bus, Ref.topic(ref))
    :ok = Bus.subscribe(bus, Ref.endpoint_topic(ref))

    _published = Bus.publish(bus, Event.new(:reply, ref, %{text: "hi"}))

    assert_receive %Event{type: :reply}, 500
    assert_receive %Event{type: :reply}, 500
  end

  test "stamps a monotonic sequence per conversation", %{ref: ref, bus: bus} do
    :ok = Bus.subscribe(bus, Ref.topic(ref))

    first = Bus.publish(bus, Event.new(:inbound, ref, %{}))
    second = Bus.publish(bus, Event.new(:reply, ref, %{text: "hi"}))

    assert second.seq == first.seq + 1
    assert Sequence.current(Ref.topic(ref)) == second.seq
  end

  test "keeps a sequence a publisher already assigned", %{ref: ref, bus: bus} do
    stamped = Bus.publish(bus, Event.new(:reply, ref, %{}, seq: 99))
    assert stamped.seq == 99
  end

  test "stops delivering after unsubscribe", %{ref: ref, bus: bus} do
    :ok = Bus.subscribe(bus, Ref.topic(ref))
    :ok = Bus.unsubscribe(bus, Ref.topic(ref))

    _published = Bus.publish(bus, Event.new(:reply, ref, %{text: "hi"}))
    refute_receive %Event{}, 100
  end

  test "normalizes the declared bus into module and options" do
    assert Bus.normalize(nil) == {Spectre.Beam.Bus.Local, []}
    assert Bus.normalize(Spectre.Beam.Bus.Local) == {Spectre.Beam.Bus.Local, []}

    assert Bus.normalize({Spectre.Beam.Bus.Local, [name: :other]}) ==
             {Spectre.Beam.Bus.Local, [name: :other]}

    assert_raise ArgumentError, ~r/invalid Beam bus/, fn -> Bus.normalize("nope") end
  end
end

defmodule Spectre.Beam.Gateway.SpecTest do
  use ExUnit.Case, async: true

  alias Spectre.Beam.Adapters.Local
  alias Spectre.Beam.Config
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Gateway.Spec

  test "builds runtime channel defaults for a reused Beam config" do
    beam = Config.new([Endpoint.new(:local, type: :local, adapter: Local)])

    assert {:ok, spec} = Spec.new(name: :reused_config, beam: beam)
    assert {:ok, channel} = Spec.channel(spec, :local)
    assert channel.ingress == :none
    assert channel.max_queue == 1_000
  end

  test "allows runtime options to accompany a reused Beam config" do
    beam = Config.new([Endpoint.new(:local, type: :local, adapter: Local)])

    assert {:ok, spec} =
             Spec.new(
               name: :configured_reuse,
               beam: beam,
               channels: [local: [coalesce_ms: 25, max_queue: 3]]
             )

    assert {:ok, %{coalesce_ms: 25, max_queue: 3}} = Spec.channel(spec, :local)
  end

  test "rehydrates provider runtime options stored in a compiled endpoint" do
    client = {__MODULE__, :client, []}

    beam =
      Config.new([
        Endpoint.new(:provider,
          type: :provider,
          adapter: Local,
          client: client,
          ingress: :subscribe,
          coalesce_ms: 25,
          max_pending: 7,
          max_queue: 11,
          overflow: :drop_oldest,
          typing: true,
          throttle: [messages_per_second: 2.0],
          retry: [max_attempts: 3]
        )
      ])

    assert {:ok, spec} = Spec.new(name: :provider_reuse, beam: beam)
    assert {:ok, channel} = Spec.channel(spec, :provider)
    assert channel.client == client
    assert channel.ingress == :subscribe
    assert channel.coalesce_ms == 25
    assert channel.max_pending == 7
    assert channel.max_queue == 11
    assert channel.overflow == :drop_oldest

    assert {:ok, endpoint} = Spec.endpoint(spec, :provider)
    assert endpoint.metadata.typing
    assert endpoint.metadata.throttle == [messages_per_second: 2.0]
    assert endpoint.metadata.retry == [max_attempts: 3]
  end

  test "rejects runtime declarations for endpoints absent from a reused config" do
    beam = Config.new([Endpoint.new(:local, type: :local, adapter: Local)])

    assert {:error, {:unknown_beam_endpoint, :typo}} =
             Spec.new(name: :invalid_reuse, beam: beam, channels: [typo: []])
  end

  test "validates settings that guard bounded queues" do
    for {key, value} <- [max_queue: 0, max_pending: -1, transcript_limit: 0, coalesce_ms: -1] do
      channel_opts = Keyword.put([type: :local, adapter: Local], key, value)

      assert {:error,
              {:invalid_beam_channel, :local, {:invalid_beam_gateway_setting, ^key, ^value}}} =
               Spec.new(
                 name: :invalid_limits,
                 channels: [local: channel_opts]
               )
    end
  end
end
