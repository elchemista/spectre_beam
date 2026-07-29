defmodule Spectre.Beam.Outbound do
  @moduledoc """
  Provider-neutral outbound delivery request.
  """

  alias Spectre.Beam.Content

  defstruct [
    :endpoint,
    :conversation_id,
    :to,
    :reply_to,
    :content,
    :idempotency_key,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          endpoint: term(),
          conversation_id: term(),
          to: term(),
          reply_to: term(),
          content: Content.t(),
          idempotency_key: String.t(),
          metadata: map()
        }

  @spec new(t() | map() | keyword()) :: t()
  def new(%__MODULE__{} = outbound), do: validate!(outbound)
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

  @spec validate!(t()) :: t()
  defp validate!(%__MODULE__{} = outbound) do
    if is_nil(outbound.endpoint), do: raise(ArgumentError, "outbound endpoint is required")

    if is_nil(outbound.to), do: raise(ArgumentError, "outbound target is required")
    unless match?(%Content{}, outbound.content), do: raise(ArgumentError, "content is required")

    unless is_binary(outbound.idempotency_key) and outbound.idempotency_key != "",
      do: raise(ArgumentError, "outbound idempotency key is required")

    unless is_map(outbound.metadata), do: raise(ArgumentError, "outbound metadata must be a map")
    outbound
  end

  @spec fields() :: [atom()]
  defp fields do
    __MODULE__.__struct__()
    |> Map.keys()
    |> List.delete(:__struct__)
  end
end
