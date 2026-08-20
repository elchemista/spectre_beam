defmodule Spectre.Beam.Event do
  @moduledoc """
  Observable gateway event delivered to every subscribed surface.

  The type set is closed and the struct is versioned, because a LiveView, an
  IEx console, a CLI, and a socket client all pattern match on it. `seq` is a
  per-conversation monotonic counter: a surface that reconnects replays from
  the last sequence it saw instead of guessing.
  """

  alias Spectre.Beam.Ref

  @version 1

  @types [
    :inbound,
    :typing,
    :delta,
    :reply,
    :receipt,
    :policy_required,
    :action,
    :status,
    :error
  ]

  defstruct [:type, :ref, :at, :seq, :payload, v: @version]

  @type type ::
          :inbound
          | :typing
          | :delta
          | :reply
          | :receipt
          | :policy_required
          | :action
          | :status
          | :error

  @type t :: %__MODULE__{
          v: pos_integer(),
          type: type(),
          ref: Ref.t(),
          at: DateTime.t(),
          seq: non_neg_integer(),
          payload: map()
        }

  @doc "Returns the closed set of event types."
  @spec types() :: [type()]
  def types, do: @types

  @doc "Returns the current event contract version."
  @spec version() :: pos_integer()
  def version, do: @version

  @doc """
  Builds an event.

      Spectre.Beam.Event.new(:reply, ref, %{text: "hello"}, seq: 7)
  """
  @spec new(type(), Ref.t(), map(), keyword()) :: t()
  def new(type, %Ref{} = ref, payload \\ %{}, opts \\ [])
      when is_map(payload) and is_list(opts) do
    unless type in @types, do: raise(ArgumentError, "unknown Beam event type: #{inspect(type)}")

    %__MODULE__{
      v: @version,
      type: type,
      ref: ref,
      at: Keyword.get(opts, :at) || DateTime.utc_now(),
      seq: Keyword.get(opts, :seq, 0),
      payload: payload
    }
  end

  @doc """
  Returns a JSON-encodable map for socket, SSE, and CLI transports.

  Terms that are not representable in JSON are printed with `inspect/1` rather
  than dropped, so a transport never silently loses an event.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = event) do
    %{
      "v" => event.v,
      "type" => Atom.to_string(event.type),
      "ref" => Ref.slug(event.ref),
      "at" => DateTime.to_iso8601(event.at),
      "seq" => event.seq,
      "payload" => encodable(event.payload)
    }
  end

  @spec encodable(term()) :: term()
  defp encodable(value) when is_map(value) and not is_struct(value) do
    Map.new(value, fn {key, inner} -> {encodable_key(key), encodable(inner)} end)
  end

  defp encodable(value) when is_list(value), do: Enum.map(value, &encodable/1)

  defp encodable(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: value

  defp encodable(value) when is_atom(value), do: Atom.to_string(value)
  defp encodable(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp encodable(value), do: inspect(value)

  @spec encodable_key(term()) :: String.t()
  defp encodable_key(key) when is_binary(key), do: key
  defp encodable_key(key) when is_atom(key), do: Atom.to_string(key)
  defp encodable_key(key), do: inspect(key)

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(event, opts) do
      concat([
        "#Beam.Event<",
        Atom.to_string(event.type),
        " ",
        Spectre.Beam.Ref.slug(event.ref),
        " #",
        Integer.to_string(event.seq),
        " ",
        to_doc(event.payload, opts),
        ">"
      ])
    end
  end
end
