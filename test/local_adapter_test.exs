defmodule Spectre.Beam.LocalAdapterTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Adapters.Local
  alias Spectre.Beam.Adapters.Test, as: TestAdapter
  alias Spectre.Beam.Content
  alias Spectre.Beam.Inbound

  test "supports scoped and fallback process attachments" do
    endpoint = :"local_#{System.unique_integer([:positive])}"
    assert :ok = Local.attach(endpoint)
    assert :ok = Local.attach(endpoint)
    assert Local.attached(endpoint) == self()
    assert Local.attached(:unknown_gateway, endpoint) == self()

    assert :ok = Local.detach(endpoint)
    assert Local.attached(endpoint) == nil

    assert :ok = Local.attach(:gateway, endpoint)
    assert Local.attached(:gateway, endpoint) == self()
    assert :ok = Local.detach(:gateway, endpoint)
  end

  test "decodes every supported local input form" do
    assert :ignore = Local.decode(:ignore, [])

    inbound =
      Inbound.new(%{
        endpoint: :console,
        message_id: "one",
        conversation_id: "one",
        sender: "me",
        content: Content.text("hello")
      })

    assert {:ok, ^inbound} = Local.decode(inbound, [])
    assert {:ok, %{content: %{text: "hello"}}} = Local.decode("hello", endpoint: :console)

    assert {:ok, decoded} =
             Local.decode(
               [text: "keyword", conversation_id: "chat", authenticated?: true],
               endpoint: :console,
               sender: "fallback"
             )

    assert decoded.sender == "fallback"
    assert decoded.authenticated?

    assert {:ok, %{content: %Content{type: :text, text: "structured"}}} =
             Local.decode(%{content: Content.text("structured")}, endpoint: :console)

    assert {:error, {:invalid_local_event, [1, 2]}} = Local.decode([1, 2], [])
    assert {:error, {:invalid_local_event, 12}} = Local.decode(12, [])
  end

  test "emits typing notifications and exposes lifecycle callbacks" do
    assert :ok = Local.typing("chat", true, endpoint: :console, notify: self())
    assert_receive {:beam_local_typing, :console, "chat", true}
    assert :ok = Local.subscribe([])
    assert :ok = Local.unsubscribe([])
  end

  test "test adapter drives inbound and delivery helpers" do
    gateway = :"test_adapter_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Spectre.Beam.Gateway, name: gateway, channels: [test: [type: :test, adapter: TestAdapter]]}
    )

    assert :ok = TestAdapter.attach(:test)
    assert {:ok, ref} = TestAdapter.send_inbound(gateway, :test, "hello")
    assert ref.endpoint == :test

    assert {:ok, ^ref} = Spectre.Beam.Gateway.push(gateway, ref, "notice")
    assert TestAdapter.next_text() == {:ok, "notice"}

    assert :ok = TestAdapter.typing("one", true, endpoint: :test, notify: self())
    assert_receive {:beam_local_typing, :test, "one", true}
    assert :ok = TestAdapter.subscribe([])
    assert :ok = TestAdapter.unsubscribe([])
    assert :ok = TestAdapter.detach(:test)
  end
end
