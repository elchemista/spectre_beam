defmodule Spectre.Beam.Bus do
  @moduledoc """
  Delivery of `Spectre.Beam.Event` values to subscribed surfaces.

  A bus is a `{module, keyword}` pair. `Spectre.Beam.Bus.Local` is the default
  and needs no dependency: it dispatches through the duplicate-key registry
  started by Beam's application. A host that already runs `Phoenix.PubSub`
  can supply an adapter over it and get cluster-wide fan-out for free.

  Publishing an event on a conversation topic also publishes it on the
  endpoint topic, so a surface can watch one chat or a whole channel without
  the publisher knowing which.
  """

  alias Spectre.Beam.Event
  alias Spectre.Beam.Ref

  @type topic ::
          {:conversation, atom() | nil, String.t()}
          | {:endpoint, atom() | nil, term()}
          | {:gateway, atom() | nil}

  @type t :: {module(), keyword()}

  @callback subscribe(topic(), keyword()) :: :ok | {:error, term()}
  @callback unsubscribe(topic(), keyword()) :: :ok
  @callback broadcast(topic(), Event.t(), keyword()) :: :ok

  @doc "Returns the default bus used when a gateway declares none."
  @spec default() :: t()
  def default, do: {Spectre.Beam.Bus.Local, []}

  @doc "Normalizes a declared bus into its `{module, opts}` form."
  @spec normalize(term()) :: t()
  def normalize(nil), do: default()
  def normalize(module) when is_atom(module), do: {module, []}

  def normalize({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}

  def normalize(invalid), do: raise(ArgumentError, "invalid Beam bus: #{inspect(invalid)}")

  @doc "Subscribes the calling process to a topic."
  @spec subscribe(t(), topic()) :: :ok | {:error, term()}
  def subscribe({module, opts}, topic), do: module.subscribe(topic, opts)

  @doc "Removes the calling process' subscription."
  @spec unsubscribe(t(), topic()) :: :ok
  def unsubscribe({module, opts}, topic), do: module.unsubscribe(topic, opts)

  @doc """
  Publishes one event on its conversation topic and its endpoint topic.

  The event is stamped with the next sequence number of its conversation
  unless it already carries one, so every subscriber sees the same order and a
  reconnecting surface can ask for what it missed.
  """
  @spec publish(t(), Event.t()) :: Event.t()
  def publish({module, opts}, %Event{ref: %Ref{} = ref} = event) do
    event = stamp(event)
    :ok = module.broadcast(Ref.topic(ref), event, opts)
    :ok = module.broadcast(Ref.endpoint_topic(ref), event, opts)
    event
  end

  @doc "Publishes one event on a single explicit topic, without stamping."
  @spec publish(t(), topic(), Event.t()) :: :ok
  def publish({module, opts}, topic, %Event{} = event), do: module.broadcast(topic, event, opts)

  @spec stamp(Event.t()) :: Event.t()
  defp stamp(%Event{seq: seq} = event) when is_integer(seq) and seq > 0, do: event

  defp stamp(%Event{ref: ref} = event),
    do: %{event | seq: Spectre.Beam.Sequence.next(Ref.topic(ref))}
end
