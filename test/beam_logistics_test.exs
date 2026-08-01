defmodule Spectre.Beam.LogisticsTest.Provider do
  @moduledoc false

  def send_typing(client, to, composing?) do
    send(client, {:provider_typing, to, composing?})
    :ok
  end

  def send_message(client, to, text) do
    send(client, {:provider_send, to, text, :no_opts})
    {:ok, "provider-3"}
  end
end

defmodule Spectre.Beam.LogisticsTest.OptsProvider do
  @moduledoc false

  def send_message(client, to, text) do
    send(client, {:provider_send, to, text, :no_opts})
    {:ok, "provider-3"}
  end

  def send_message(client, to, text, send_opts) do
    send(client, {:provider_send, to, text, send_opts})
    {:ok, "provider-4"}
  end
end

defmodule Spectre.Beam.LogisticsTest.RecordingAdapter do
  @moduledoc false

  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Receipt

  @impl true
  def capabilities(_opts), do: [:text]

  @impl true
  def decode(_event, _opts), do: :ignore

  @impl true
  def typing(to, composing?, opts) do
    send(Keyword.fetch!(opts, :test_pid), {:adapter_typing, to, composing?})
    Keyword.get(opts, :typing_reply, :ok)
  end

  @impl true
  def deliver(outbound, opts) do
    counter = Keyword.fetch!(opts, :counter)
    :counters.add(counter, 1, 1)
    attempt = :counters.get(counter, 1)

    send(Keyword.fetch!(opts, :test_pid), {:adapter_delivered, outbound.idempotency_key, attempt})

    if attempt <= Keyword.get(opts, :fail_until, 0) do
      Keyword.get(opts, :failure, {:error, :transient_provider_error})
    else
      {:ok, Receipt.accepted(outbound, provider_message_id: "ok")}
    end
  end
end

defmodule Spectre.Beam.LogisticsTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Adapters.ExGram
  alias Spectre.Beam.Adapters.ExWapp
  alias Spectre.Beam.Content
  alias Spectre.Beam.LogisticsTest.OptsProvider
  alias Spectre.Beam.LogisticsTest.Provider
  alias Spectre.Beam.LogisticsTest.RecordingAdapter
  alias Spectre.Beam.Receipt
  alias Spectre.Beam.Store
  alias Spectre.Beam.Throttle.Local

  setup do
    Store.reset()
    Local.reset()
    :ok
  end

  defp beam(channel_opts) do
    Spectre.Beam.new(
      main: Keyword.merge([type: :whatsapp, adapter: RecordingAdapter], channel_opts)
    )
  end

  defp outbound(key) do
    %{
      conversation_id: "conv-1",
      to: "user-1",
      content: Content.text("hello"),
      idempotency_key: key
    }
  end

  defp deliver(beam, key, opts \\ []) do
    counter = Keyword.get_lazy(opts, :counter, fn -> :counters.new(1, []) end)

    adapter_opts =
      [test_pid: self(), counter: counter]
      |> Keyword.merge(Keyword.get(opts, :adapter_opts, []))

    Spectre.Beam.deliver(
      beam,
      :main,
      outbound(key),
      Keyword.merge([adapter_opts: adapter_opts], Keyword.drop(opts, [:adapter_opts, :counter]))
    )
  end

  test "typing fires before the provider send and stays best effort" do
    beam = beam(typing: true)

    assert {:ok, %Receipt{}} = deliver(beam, "typing-1")
    assert_receive {:adapter_typing, "user-1", true}
    assert_receive {:adapter_delivered, "typing-1", 1}

    # A failing typing callback must never fail the delivery.
    assert {:ok, %Receipt{}} =
             deliver(beam, "typing-2", adapter_opts: [typing_reply: {:error, :boom}])

    assert_receive {:adapter_typing, "user-1", true}
    assert_receive {:adapter_delivered, "typing-2", 1}
  end

  test "reply delay pauses between typing and the provider call" do
    beam = beam(typing: true, reply_delay_ms: 80)
    started_at = System.monotonic_time(:millisecond)

    assert {:ok, %Receipt{}} = deliver(beam, "delayed-1")
    assert System.monotonic_time(:millisecond) - started_at >= 80

    assert_receive {:adapter_typing, "user-1", true}
    assert_receive {:adapter_delivered, "delayed-1", 1}
  end

  test "per-call options override endpoint logistics" do
    beam = beam(reply_delay_ms: 5_000)
    started_at = System.monotonic_time(:millisecond)

    assert {:ok, %Receipt{}} = deliver(beam, "override-1", reply_delay_ms: 0)
    assert System.monotonic_time(:millisecond) - started_at < 1_000
  end

  test "retry policy retries plain errors with backoff and returns the receipt" do
    beam = beam(retry: [max_attempts: 3, base_delay_ms: 10])

    assert {:ok, %Receipt{provider_message_id: "ok"}} =
             deliver(beam, "retry-1", adapter_opts: [fail_until: 2])

    assert_receive {:adapter_delivered, "retry-1", 1}
    assert_receive {:adapter_delivered, "retry-1", 2}
    assert_receive {:adapter_delivered, "retry-1", 3}
  end

  test "exhausted retries release the claim so a later send may proceed" do
    beam = beam(retry: [max_attempts: 2, base_delay_ms: 10])
    counter = :counters.new(1, [])

    assert {:error, :transient_provider_error} =
             deliver(beam, "retry-2", counter: counter, adapter_opts: [fail_until: 99])

    assert {:ok, %Receipt{}} =
             deliver(beam, "retry-2", counter: counter, adapter_opts: [fail_until: 2])
  end

  test "ambiguous provider outcomes are never retried and keep the claim" do
    beam = beam(retry: [max_attempts: 5, base_delay_ms: 10])
    counter = :counters.new(1, [])

    assert {:error, {:ambiguous, :ack_timeout}} =
             deliver(beam, "ambiguous-1",
               counter: counter,
               adapter_opts: [fail_until: 99, failure: {:error, {:ambiguous, :ack_timeout}}]
             )

    assert :counters.get(counter, 1) == 1

    # The claim is retained: a second identical send is fenced out.
    assert {:error, {:beam_delivery_in_progress, "ambiguous-1"}} =
             deliver(beam, "ambiguous-1", counter: counter)
  end

  test "retry_on narrows which errors are retryable" do
    beam = beam(retry: [max_attempts: 3, base_delay_ms: 10, retry_on: &(&1 == :other)])
    counter = :counters.new(1, [])

    assert {:error, :transient_provider_error} =
             deliver(beam, "retry-3", counter: counter, adapter_opts: [fail_until: 99])

    assert :counters.get(counter, 1) == 1
  end

  test "endpoint throttle spaces consecutive sends" do
    beam = beam(throttle: [min_delay_ms: 90])
    started_at = System.monotonic_time(:millisecond)

    assert {:ok, %Receipt{}} = deliver(beam, "paced-1")
    assert {:ok, %Receipt{}} = deliver(beam, "paced-2")

    assert System.monotonic_time(:millisecond) - started_at >= 90
  end

  test "throttle on_limit :error rejects instead of waiting and releases the claim" do
    beam = beam(throttle: [min_delay_ms: 5_000, on_limit: :error])

    assert {:ok, %Receipt{}} = deliver(beam, "limited-1")

    assert {:error, {:beam_throttled, :main, {:beam_throttle_limit, _wait}}} =
             deliver(beam, "limited-2")

    # The claim was released, so the same send succeeds once pacing allows it.
    Local.reset()
    assert {:ok, %Receipt{}} = deliver(beam, "limited-2")
  end

  test "saturated throttle rejects reservations beyond max_wait_ms" do
    beam = beam(throttle: [min_delay_ms: 400, max_wait_ms: 100])

    assert {:ok, %Receipt{}} = deliver(beam, "saturated-1")

    assert {:error, {:beam_throttled, :main, {:beam_throttle_saturated, _wait}}} =
             deliver(beam, "saturated-2")
  end

  test "Throttle.Local paces per conversation and per endpoint bucket" do
    config = [per_conversation: [min_delay_ms: 200]]

    assert :ok = Local.reserve({:wa, "a"}, config, [])
    assert {:wait, wait} = Local.reserve({:wa, "a"}, config, [])
    assert wait > 0 and wait <= 200
    assert :ok = Local.reserve({:wa, "b"}, config, [])

    bucket = [messages_per_second: 10.0, burst: 2]
    assert :ok = Local.reserve({:tg, "a"}, bucket, [])
    assert :ok = Local.reserve({:tg, "b"}, bucket, [])
    assert {:wait, bucket_wait} = Local.reserve({:tg, "c"}, bucket, [])
    assert bucket_wait > 0 and bucket_wait <= 150
  end

  test "logistics options do not leak into adapter opts" do
    endpoint =
      Spectre.Beam.Endpoint.new(:main,
        adapter: RecordingAdapter,
        typing: true,
        reply_delay_ms: 10,
        retry: [max_attempts: 2],
        throttle: [min_delay_ms: 10],
        custom: :kept
      )

    assert endpoint.opts == [custom: :kept]
    assert endpoint.metadata.typing == true
    assert endpoint.metadata.reply_delay_ms == 10
    assert endpoint.metadata.retry == [max_attempts: 2]
    assert endpoint.metadata.throttle == [min_delay_ms: 10]
  end

  test "bundled adapters expose provider typing" do
    opts = [client: self(), module: Provider]

    assert :ok = ExWapp.typing("jid-1", true, opts)
    assert_receive {:provider_typing, "jid-1", true}

    assert :ok = ExGram.typing(42, false, opts)
    assert_receive {:provider_typing, 42, false}
  end

  test "text sends pass send_opts when the provider supports them" do
    outbound =
      Spectre.Beam.Outbound.new(%{
        endpoint: :main,
        conversation_id: "conv-1",
        to: "user-1",
        content: Content.text("ciao"),
        reply_to: "msg-9",
        idempotency_key: "opts-1"
      })

    assert {:ok, %Receipt{provider_message_id: "provider-4"}} =
             ExGram.deliver(outbound,
               client: self(),
               module: OptsProvider,
               send_opts: [parse_mode: "MarkdownV2"]
             )

    assert_receive {:provider_send, "user-1", "ciao", send_opts}
    assert send_opts[:parse_mode] == "MarkdownV2"
    assert send_opts[:reply_to] == "msg-9"

    # Providers without the wider arity keep receiving the plain call.
    assert {:ok, %Receipt{provider_message_id: "provider-3"}} =
             ExWapp.deliver(outbound,
               client: self(),
               module: Provider,
               send_opts: [something: true]
             )

    assert_receive {:provider_send, "user-1", "ciao", :no_opts}
  end
end
