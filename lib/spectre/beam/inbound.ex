defmodule Spectre.Beam.Inbound do
  @moduledoc """
  Normalized inbound event returned by a Beam channel adapter.
  """

  alias Spectre.Beam.Content

  defstruct [
    :endpoint,
    :channel_type,
    :message_id,
    :conversation_id,
    :sender,
    :recipient,
    :content,
    :occurred_at,
    authenticated?: false,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          endpoint: term(),
          channel_type: term(),
          message_id: String.t(),
          conversation_id: term(),
          sender: term(),
          recipient: term(),
          content: Content.t(),
          authenticated?: boolean(),
          occurred_at: DateTime.t() | nil,
          metadata: map()
        }

  @spec new(t() | map() | keyword()) :: t()
  def new(%__MODULE__{} = inbound), do: validate!(inbound)
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    attrs =
      case Map.get(attrs, :content) do
        %Content{} ->
          attrs

        content when is_map(content) or is_list(content) ->
          Map.put(attrs, :content, Content.new(content))

        _invalid ->
          attrs
      end

    attrs
    |> then(&struct(__MODULE__, Map.take(&1, fields())))
    |> validate!()
  end

  @spec key(t()) :: {term(), String.t()}
  def key(%__MODULE__{} = inbound), do: {inbound.endpoint, inbound.message_id}

  @spec conversation_key(t()) :: {:beam, term(), term()}
  def conversation_key(%__MODULE__{} = inbound),
    do: {:beam, inbound.endpoint, inbound.conversation_id}

  @spec validate!(t()) :: t()
  defp validate!(%__MODULE__{} = inbound) do
    unless is_binary(inbound.message_id) and inbound.message_id != "",
      do: raise(ArgumentError, "Beam inbound message_id is required")

    if is_nil(inbound.conversation_id),
      do: raise(ArgumentError, "Beam inbound conversation_id is required")

    unless match?(%Content{}, inbound.content),
      do: raise(ArgumentError, "Beam inbound content is required")

    unless is_boolean(inbound.authenticated?),
      do: raise(ArgumentError, "Beam authenticated? must be boolean")

    unless is_map(inbound.metadata),
      do: raise(ArgumentError, "Beam inbound metadata must be a map")

    inbound
  end

  @spec fields() :: [atom()]
  defp fields do
    __MODULE__.__struct__()
    |> Map.keys()
    |> List.delete(:__struct__)
  end
end
