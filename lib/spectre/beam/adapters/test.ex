defmodule Spectre.Beam.Adapters.Test do
  @moduledoc """
  Channel for exercising a gateway end to end from a test.

  It is `Spectre.Beam.Adapters.Local` plus the two helpers a test needs:
  attaching automatically and blocking for the next delivery. Because it is a
  real channel, a test drives the same pipelines, claims, and logistics that
  production runs — not a parallel code path.

      setup do
        start_supervised!(
          {Spectre.Beam.Gateway,
           name: :test_gateway,
           agent: MyAgent,
           channels: [chat: [type: :test, adapter: Spectre.Beam.Adapters.Test]]}
        )

        :ok = Spectre.Beam.Adapters.Test.attach(:chat)
      end

      test "answers" do
        {:ok, _ref} = Spectre.Beam.Adapters.Test.send_inbound(:test_gateway, :chat, "hello")
        assert {:ok, outbound} = Spectre.Beam.Adapters.Test.next_delivery()
        assert outbound.content.text =~ "hi"
      end
  """

  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Adapters.Local

  @default_timeout 1_000

  @doc "Registers the calling process as the receiver of an endpoint's deliveries."
  @spec attach(atom() | term(), term() | nil) :: :ok | {:error, term()}
  defdelegate attach(gateway_or_endpoint, endpoint \\ nil), to: Local

  @doc "Removes the calling process' attachment."
  @spec detach(atom() | term(), term() | nil) :: :ok
  defdelegate detach(gateway_or_endpoint, endpoint \\ nil), to: Local

  @doc """
  Feeds one event into a running gateway endpoint.

  Returns the conversation reference the event was routed to.
  """
  @spec send_inbound(atom(), term(), term(), keyword()) ::
          {:ok, Spectre.Beam.Ref.t()} | :ignore | {:error, term()}
  def send_inbound(gateway, endpoint, event, opts \\ []) do
    Spectre.Beam.Gateway.ingest(gateway, endpoint, event, opts)
  end

  @doc """
  Waits for the next delivery on any attached endpoint.

  Returns `{:ok, outbound}`, or `{:error, :timeout}` when none arrives.
  """
  @spec next_delivery(timeout()) :: {:ok, Spectre.Beam.Outbound.t()} | {:error, :timeout}
  def next_delivery(timeout \\ @default_timeout) do
    receive do
      {:beam_local, _endpoint, outbound} -> {:ok, outbound}
    after
      timeout -> {:error, :timeout}
    end
  end

  @doc "Waits for the next delivery text on any attached endpoint."
  @spec next_text(timeout()) :: {:ok, String.t()} | {:error, :timeout}
  def next_text(timeout \\ @default_timeout) do
    with {:ok, outbound} <- next_delivery(timeout), do: {:ok, outbound.content.text}
  end

  @impl Spectre.Beam.Channel
  defdelegate capabilities(opts), to: Local

  @impl Spectre.Beam.Channel
  defdelegate decode(event, opts), to: Local

  @impl Spectre.Beam.Channel
  defdelegate deliver(outbound, opts), to: Local

  @impl Spectre.Beam.Channel
  defdelegate typing(to, composing?, opts), to: Local

  @impl Spectre.Beam.Channel
  defdelegate subscribe(opts), to: Local

  @impl Spectre.Beam.Channel
  defdelegate unsubscribe(opts), to: Local
end
