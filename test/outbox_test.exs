defmodule Spectre.Beam.OutboxTest.BlockingAdapter do
  @moduledoc false
  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Adapters.Local

  def capabilities(_opts), do: [:text]
  def decode(event, opts), do: Local.decode(event, opts)

  def deliver(outbound, opts) do
    owner = opts[:client]
    send(owner, {:delivery_started, self(), outbound})

    receive do
      :release -> Local.deliver(outbound, opts)
      :fail -> {:error, :provider_down}
      :crash -> exit(:provider_crashed)
    end
  end
end

defmodule Spectre.Beam.OutboxTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Bus
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Outbox
  alias Spectre.Beam.Ref

  defp start_gateway(overflow) do
    name = :"outbox_#{overflow}_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Gateway,
       name: name,
       channels: [
         slow: [
           type: :local,
           adapter: Spectre.Beam.OutboxTest.BlockingAdapter,
           client: self(),
           max_queue: 1,
           overflow: overflow
         ]
       ]}
    )

    name
  end

  test "rejects work after the bounded queue fills and drain can time out" do
    gateway = start_gateway(:reject)

    assert {:ok, _} = Gateway.push(gateway, "slow:one", "first")
    assert_receive {:delivery_started, worker, _outbound}
    assert {:ok, _} = Gateway.push(gateway, "slow:two", "second")

    assert {:error, {:beam_outbox_full, :slow, 1}} =
             Gateway.push(gateway, "slow:three", "third")

    assert {:error, :timeout} = Outbox.drain(gateway, :slow, 0)
    assert {:ok, %{queued: 1, inflight: 1}} = Outbox.info(gateway, :slow)

    send(worker, :release)
    assert_receive {:delivery_started, second_worker, _outbound}, 1_000
    send(second_worker, :release)
    assert :ok = Outbox.drain(gateway, :slow)
    assert {:ok, %{delivered: 2, failed: 0}} = Outbox.info(gateway, :slow)

    assert Outbox.info(gateway, :missing) == {:error, :not_found}
  end

  test "drop_oldest reports the shed message and delivers the replacement" do
    gateway = start_gateway(:drop_oldest)
    {:ok, dropped_ref} = Gateway.open(gateway, "slow:dropped")
    :ok = Bus.subscribe(Bus.default(), Ref.topic(dropped_ref))

    assert {:ok, _} = Gateway.push(gateway, "slow:first", "first")
    assert_receive {:delivery_started, worker, _}
    assert {:ok, ^dropped_ref} = Gateway.push(gateway, dropped_ref, "dropped")
    assert {:ok, _} = Gateway.push(gateway, "slow:replacement", "replacement")

    assert_receive %Event{type: :error, payload: %{reason: :beam_outbox_overflow}}, 1_000

    send(worker, :release)
    assert_receive {:delivery_started, replacement_worker, outbound}, 1_000
    assert outbound.content.text == "replacement"
    send(replacement_worker, :release)
    assert :ok = Outbox.drain(gateway, :slow)
  end

  test "reports provider errors and task crashes" do
    gateway = start_gateway(:reject)

    for {conversation, result} <- [{"failure", :fail}, {"crash", :crash}] do
      {:ok, ref} = Gateway.open(gateway, "slow:#{conversation}")
      :ok = Bus.subscribe(Bus.default(), Ref.topic(ref))
      assert {:ok, ^ref} = Gateway.push(gateway, ref, conversation)
      assert_receive {:delivery_started, worker, _}
      send(worker, result)
      assert_receive %Event{type: :error, payload: %{stage: :deliver}}, 1_000
    end

    assert :ok = Outbox.drain(gateway, :slow)
    assert {:ok, %{failed: 2}} = Outbox.info(gateway, :slow)

    outbound =
      Spectre.Beam.Outbound.new(%{
        endpoint: :missing,
        conversation_id: "one",
        to: "one",
        content: Spectre.Beam.Content.text("hi"),
        idempotency_key: "missing"
      })

    assert {:error, {:beam_outbox_unavailable, :missing}} =
             Outbox.enqueue(gateway, :missing, outbound)
  end
end
