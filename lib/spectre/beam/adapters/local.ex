defmodule Spectre.Beam.Adapters.Local do
  @moduledoc """
  In-VM channel for surfaces that live inside the node.

  The IEx console, a LiveView chat, the CLI over the local socket, and the
  test suite are all *channels*, not a second API next to the provider ones.
  They decode a plain term into an inbound and deliver by handing the outbound
  back to the node — which means every pipeline, idempotency claim, throttle,
  and policy that guards Telegram guards them identically.

  For a gateway conversation the authoritative delivery path is the event bus:
  the gateway already publishes `:reply` and `:receipt` events every subscriber
  sees. `deliver/2` additionally notifies an attached process, which is what
  makes a blocking `Spectre.Beam.Chat.ask/3` and test assertions possible.

      iex> Spectre.Beam.Adapters.Local.attach(:console)
      :ok

  Accepted events:

    * a binary — its text, with generated ids;
    * a map or keyword list of `Spectre.Beam.Inbound` fields;
    * an existing `Spectre.Beam.Inbound`;
    * `:ignore`, to exercise the ignore path.
  """

  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Content
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Receipt

  @registry Spectre.Beam.Registry

  @default_conversation "local"
  @default_sender "local"

  @doc """
  Registers the calling process as the receiver of an endpoint's deliveries.

  Delivered messages arrive as `{:beam_local, endpoint, outbound}` and typing
  signals as `{:beam_local_typing, endpoint, to, composing?}`.

  Attachment is scoped to a gateway. Two gateways that both mount a `:console`
  channel are two different channels, and a delivery in flight when one of
  them stops must never land in the other's listener.
  """
  @spec attach(atom() | term(), term() | nil) :: :ok | {:error, term()}
  def attach(gateway_or_endpoint, endpoint \\ nil)

  def attach(endpoint, nil), do: register({nil, endpoint})
  def attach(gateway, endpoint), do: register({gateway, endpoint})

  @doc "Removes the calling process' attachment."
  @spec detach(atom() | term(), term() | nil) :: :ok
  def detach(gateway_or_endpoint, endpoint \\ nil)

  def detach(endpoint, nil), do: unregister({nil, endpoint})
  def detach(gateway, endpoint), do: unregister({gateway, endpoint})

  @doc """
  Returns the process attached to an endpoint, if any.

  A gateway-scoped attachment wins; an unscoped one is the fallback, which is
  what a plain `attach(:console)` from a script or a test relies on.
  """
  @spec attached(atom() | term(), term() | nil) :: pid() | nil
  def attached(gateway_or_endpoint, endpoint \\ nil)

  def attached(endpoint, nil), do: lookup({nil, endpoint})

  def attached(gateway, endpoint), do: lookup({gateway, endpoint}) || lookup({nil, endpoint})

  @spec register({atom() | nil, term()}) :: :ok | {:error, term()}
  defp register(scope) do
    case Registry.register(@registry, {:local_channel, scope}, nil) do
      {:ok, _owner} -> :ok
      {:error, {:already_registered, pid}} when pid == self() -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec unregister({atom() | nil, term()}) :: :ok
  defp unregister(scope) do
    Registry.unregister(@registry, {:local_channel, scope})
    :ok
  end

  @spec lookup({atom() | nil, term()}) :: pid() | nil
  defp lookup(scope) do
    case Registry.lookup(@registry, {:local_channel, scope}) do
      [{pid, _value}] -> pid
      [] -> nil
    end
  end

  @impl Spectre.Beam.Channel
  def capabilities(opts), do: Keyword.get(opts, :capabilities, [:text])

  @impl Spectre.Beam.Channel
  def decode(:ignore, _opts), do: :ignore

  def decode(%Inbound{} = inbound, _opts), do: {:ok, inbound}

  def decode(text, opts) when is_binary(text), do: decode(%{text: text}, opts)

  def decode(event, opts) when is_list(event) do
    if Keyword.keyword?(event),
      do: event |> Map.new() |> decode(opts),
      else: {:error, {:invalid_local_event, event}}
  end

  def decode(event, opts) when is_map(event) do
    {:ok,
     Inbound.new(%{
       message_id: field(event, opts, :message_id) || generated_id(),
       conversation_id: field(event, opts, :conversation_id) || @default_conversation,
       sender: field(event, opts, :sender) || @default_sender,
       recipient: field(event, opts, :recipient),
       content: content(event),
       authenticated?: field(event, opts, :authenticated?) || false,
       occurred_at: Map.get(event, :occurred_at) || DateTime.utc_now(),
       metadata: Map.get(event, :metadata, %{})
     })}
  rescue
    exception in ArgumentError -> {:error, {:invalid_local_event, Exception.message(exception)}}
  end

  def decode(event, _opts), do: {:error, {:invalid_local_event, event}}

  @impl Spectre.Beam.Channel
  def deliver(outbound, opts) do
    notify(outbound.endpoint, opts, {:beam_local, outbound.endpoint, outbound})

    {:ok,
     Receipt.accepted(outbound,
       provider_message_id: "local-" <> outbound.idempotency_key,
       metadata: %{transport: :local}
     )}
  end

  @impl Spectre.Beam.Channel
  def typing(to, composing?, opts) do
    endpoint = Keyword.get(opts, :endpoint)
    notify(endpoint, opts, {:beam_local_typing, endpoint, to, composing?})
    :ok
  end

  @impl Spectre.Beam.Channel
  def subscribe(_opts), do: :ok

  @impl Spectre.Beam.Channel
  def unsubscribe(_opts), do: :ok

  # An explicit field on the event wins over the endpoint's configured default,
  # which is what lets one attached console serve several conversations.
  @spec field(map(), keyword(), atom()) :: term()
  defp field(event, opts, key), do: Map.get(event, key) || Keyword.get(opts, key)

  @spec content(map()) :: Content.t()
  defp content(event) do
    case Map.get(event, :content) do
      %Content{} = content -> content
      nil -> Content.text(Map.get(event, :text) || "")
      other -> Content.new(other)
    end
  end

  @spec notify(term(), keyword(), term()) :: :ok
  defp notify(endpoint, opts, message) do
    listener = Keyword.get(opts, :notify) || attached(Keyword.get(opts, :gateway), endpoint)

    case listener do
      pid when is_pid(pid) -> send(pid, message)
      _none -> :ok
    end

    :ok
  end

  @spec generated_id() :: String.t()
  defp generated_id, do: "local-" <> Integer.to_string(System.unique_integer([:positive]))
end
