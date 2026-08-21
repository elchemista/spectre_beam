defmodule Spectre.Beam.Ref do
  @moduledoc """
  Stable address of one gateway conversation.

  A reference names the gateway, the mounted endpoint, and the provider
  conversation. It is the registry key of a `Spectre.Beam.Conversation`, the
  bus topic every surface subscribes to, and the value a CLI or IEx caller
  types as `"telegram:12345"`.

  `conversation_id` keeps the provider's own term, so an outbound built from a
  reference addresses the provider exactly as the inbound did. `slug/1`
  derives the printable form used for lookup and display. Integer and binary
  conversation ids share a slug, so `parse!/2` addresses a live conversation
  started from either.
  """

  alias Spectre.Beam.Inbound

  defstruct [:gateway, :endpoint, :conversation_id, :agent, scope: :session]

  @type scope :: :session | :instance

  @type t :: %__MODULE__{
          gateway: atom() | nil,
          endpoint: atom() | String.t(),
          conversation_id: term(),
          agent: module() | nil,
          scope: scope()
        }

  @doc """
  Builds a reference.

      Spectre.Beam.Ref.new(endpoint: :telegram, conversation_id: 12_345)
  """
  @spec new(keyword() | map()) :: t()
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    attrs
    |> Map.take(fields())
    |> validate!()
    |> then(&struct(__MODULE__, &1))
  end

  @doc "Builds a reference for one normalized inbound event."
  @spec from_inbound(Inbound.t(), keyword()) :: t()
  def from_inbound(%Inbound{} = inbound, opts \\ []) when is_list(opts) do
    new(%{
      gateway: Keyword.get(opts, :gateway),
      endpoint: inbound.endpoint,
      conversation_id: inbound.conversation_id,
      agent: Keyword.get(opts, :agent),
      scope: Keyword.get(opts, :scope, :session)
    })
  end

  @doc """
  Parses the printable `"endpoint:conversation"` form.

  The conversation id of a parsed reference is always a binary. Its slug still
  matches a conversation started from the equivalent integer id, so parsing is
  a safe way to address a live conversation from a terminal.
  """
  @spec parse(String.t() | t(), keyword()) :: {:ok, t()} | {:error, term()}
  def parse(value, opts \\ [])

  def parse(%__MODULE__{} = ref, _opts), do: {:ok, ref}

  def parse(value, opts) when is_binary(value) and is_list(opts) do
    case String.split(value, ":", parts: 2) do
      [endpoint, conversation] when endpoint != "" and conversation != "" ->
        {:ok,
         new(%{
           gateway: Keyword.get(opts, :gateway),
           endpoint: endpoint_id(endpoint, opts),
           conversation_id: conversation,
           agent: Keyword.get(opts, :agent),
           scope: Keyword.get(opts, :scope, :session)
         })}

      _invalid ->
        {:error, {:invalid_beam_ref, value}}
    end
  end

  def parse(value, _opts), do: {:error, {:invalid_beam_ref, value}}

  @doc "Parses the printable form, raising on an invalid value."
  @spec parse!(String.t() | t(), keyword()) :: t()
  def parse!(value, opts \\ []) do
    case parse(value, opts) do
      {:ok, ref} -> ref
      {:error, reason} -> raise ArgumentError, "invalid Beam reference: #{inspect(reason)}"
    end
  end

  @doc """
  Returns the printable, lookup-stable form of a reference.

      iex> Spectre.Beam.Ref.slug(Spectre.Beam.Ref.new(endpoint: :telegram, conversation_id: 42))
      "telegram:42"
  """
  @spec slug(t()) :: String.t()
  def slug(%__MODULE__{} = ref),
    do: printable(ref.endpoint) <> ":" <> printable(ref.conversation_id)

  @doc "Returns the registry key used by the conversation process."
  @spec key(t()) :: {:conversation, atom() | nil, String.t()}
  def key(%__MODULE__{} = ref), do: {:conversation, ref.gateway, slug(ref)}

  @doc "Returns the bus topic carrying every event of one conversation."
  @spec topic(t()) :: {:conversation, atom() | nil, String.t()}
  def topic(%__MODULE__{} = ref), do: key(ref)

  @doc "Returns the bus topic carrying every event of one endpoint."
  @spec endpoint_topic(t()) :: {:endpoint, atom() | nil, term()}
  def endpoint_topic(%__MODULE__{} = ref), do: {:endpoint, ref.gateway, ref.endpoint}

  @spec endpoint_id(String.t(), keyword()) :: atom() | String.t()
  defp endpoint_id(endpoint, opts) do
    case Keyword.get(opts, :endpoints) do
      known when is_list(known) ->
        Enum.find(known, endpoint, &(printable(&1) == endpoint))

      _unknown ->
        endpoint
    end
  end

  @spec printable(term()) :: String.t()
  defp printable(value) when is_binary(value), do: value
  defp printable(value) when is_atom(value), do: Atom.to_string(value)
  defp printable(value) when is_integer(value), do: Integer.to_string(value)
  defp printable(value), do: inspect(value)

  # Validation runs on the attributes rather than the built struct: that is the
  # only point where a caller's value is still arbitrary, and where rejecting
  # it produces a useful message instead of a later mismatch.
  @spec validate!(map()) :: map()
  defp validate!(attrs) do
    unless is_atom(Map.get(attrs, :gateway)),
      do: raise(ArgumentError, "Beam ref gateway must be an atom")

    unless valid_endpoint?(Map.get(attrs, :endpoint)),
      do: raise(ArgumentError, "Beam ref endpoint is required")

    if is_nil(Map.get(attrs, :conversation_id)),
      do: raise(ArgumentError, "Beam ref conversation_id is required")

    unless is_atom(Map.get(attrs, :agent)),
      do: raise(ArgumentError, "Beam ref agent must be a module")

    unless Map.get(attrs, :scope, :session) in [:session, :instance],
      do: raise(ArgumentError, "Beam ref scope must be :session or :instance")

    attrs
  end

  @spec valid_endpoint?(term()) :: boolean()
  defp valid_endpoint?(endpoint) when is_atom(endpoint), do: not is_nil(endpoint)
  defp valid_endpoint?(endpoint) when is_binary(endpoint), do: endpoint != ""
  defp valid_endpoint?(_endpoint), do: false

  @spec fields() :: [atom()]
  defp fields do
    __MODULE__.__struct__()
    |> Map.keys()
    |> List.delete(:__struct__)
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(ref, _opts) do
      concat(["#Beam.Ref<", Spectre.Beam.Ref.slug(ref), ">"])
    end
  end
end
