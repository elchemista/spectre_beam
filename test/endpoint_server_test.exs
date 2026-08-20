defmodule Spectre.Beam.EndpointServerTest.Adapter do
  @moduledoc false
  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Adapters.Local

  def capabilities(_opts), do: [:text]
  def decode(event, opts), do: Local.decode(event, opts)
  def deliver(outbound, opts), do: Local.deliver(outbound, opts)

  def subscribe(opts) do
    send(opts[:notify], {:lifecycle, :subscribe, opts[:client]})
    if opts[:client] == :reject, do: {:error, :rejected}, else: :ok
  end

  def unsubscribe(opts) do
    send(opts[:notify], {:lifecycle, :unsubscribe, opts[:client]})
    :ok
  end
end

defmodule Spectre.Beam.EndpointServerTest.Fetcher do
  @moduledoc false

  def client(value), do: {:ok, value}
  def explode_client, do: raise("client exploded")

  def fetch(owner, result, opts) do
    send(owner, {:polled, opts[:client]})
    result
  end

  def crash(_opts), do: exit(:poll_crashed)
end

defmodule Spectre.Beam.EndpointServerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Spectre.Beam.Endpoint.Server
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Ref

  defp start_gateway(channel_opts) do
    name = :"endpoint_server_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {Gateway,
         name: name,
         channels: [
           inbox:
             Keyword.merge(
               [type: :local, adapter: Spectre.Beam.EndpointServerTest.Adapter, notify: self()],
               channel_opts
             )
         ]}
      )

    {name, pid}
  end

  test "resolves clients and runs subscription lifecycle" do
    {gateway, pid} =
      start_gateway(
        client: {Spectre.Beam.EndpointServerTest.Fetcher, :client, [:session]},
        ingress: :subscribe
      )

    assert_receive {:lifecycle, :subscribe, :session}

    assert {:ok, %{ingress: :subscribe, subscribed?: true, status: :up}} =
             Server.status(gateway, :inbox)

    assert Server.adapter_opts(gateway, :inbox)[:client] == :session
    assert Server.adapter_opts(gateway, :inbox)[:notify] == self()

    GenServer.cast(Server.name(gateway, :inbox), :record_event)
    assert_eventually(fn -> match?({:ok, %{events: 1}}, Server.status(gateway, :inbox)) end)

    GenServer.stop(pid)
    assert_receive {:lifecycle, :unsubscribe, :session}
    assert Server.status(gateway, :inbox) == {:error, :not_found}
    assert Server.adapter_opts(gateway, :inbox) == []
    assert Server.record_event(gateway, :inbox) == :ok
  end

  test "marks a rejected subscription as degraded" do
    {gateway, _pid} = start_gateway(client: fn -> :reject end, ingress: :subscribe)
    assert_receive {:lifecycle, :subscribe, :reject}

    assert {:ok, %{subscribed?: false, status: {:degraded, :rejected}}} =
             Server.status(gateway, :inbox)
  end

  test "polls through function and MFA fetchers and ingests returned events" do
    owner = self()

    fetch = fn opts ->
      send(owner, {:function_poll, opts[:client]})
      {:ok, [%{text: "hello", conversation_id: "polled", message_id: "poll-1"}]}
    end

    {gateway, _pid} =
      start_gateway(client: :poll_client, ingress: {:poll, [fetch: fetch, interval_ms: 60_000]})

    {:ok, ref} = Gateway.open(gateway, "inbox:polled")
    :ok = Spectre.Beam.Bus.subscribe(Spectre.Beam.Bus.default(), Ref.topic(ref))
    assert_receive {:function_poll, :poll_client}, 1_000
    assert_receive %Event{type: :inbound, payload: %{text: "hello"}}, 1_000

    {other, _pid} =
      start_gateway(
        client: :mfa_client,
        ingress:
          {:poll,
           [
             fetch: {Spectre.Beam.EndpointServerTest.Fetcher, :fetch, [owner, :ignore]},
             subscribe: true,
             interval_ms: 60_000
           ]}
      )

    assert_receive {:lifecycle, :subscribe, :mfa_client}
    assert_receive {:polled, :mfa_client}, 1_000
    assert {:ok, %{ingress: :poll, subscribed?: true}} = Server.status(other, :inbox)
  end

  test "recovers from error, invalid and crashing poll results" do
    owner = self()

    for result <- [:ok, {:error, :offline}, :unexpected] do
      {gateway, _pid} =
        start_gateway(
          client: result,
          ingress:
            {:poll,
             [
               fetch: {Spectre.Beam.EndpointServerTest.Fetcher, :fetch, [owner, result]},
               interval_ms: 60_000
             ]}
        )

      assert_receive {:polled, ^result}, 1_000
      Process.sleep(20)

      assert {:ok, %{status: :up}} = Server.status(gateway, :inbox)
    end

    {_gateway, _pid} =
      start_gateway(
        ingress:
          {:poll,
           [fetch: {Spectre.Beam.EndpointServerTest.Fetcher, :crash, []}, interval_ms: 60_000]}
      )

    _log = capture_log(fn -> Process.sleep(50) end)
  end

  test "rejects a client resolver error without leaving an endpoint process" do
    name = :"bad_endpoint_#{System.unique_integer([:positive])}"

    assert_raise RuntimeError, ~r/failed to start child/, fn ->
      start_supervised!(
        {Gateway,
         name: name,
         channels: [
           inbox: [
             type: :local,
             adapter: Spectre.Beam.EndpointServerTest.Adapter,
             client: {Spectre.Beam.EndpointServerTest.Fetcher, :explode_client, []}
           ]
         ]}
      )
    end
  end

  defp assert_eventually(fun, attempts \\ 20)
  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end
end
