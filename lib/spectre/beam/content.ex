defmodule Spectre.Beam.Content do
  @moduledoc """
  Provider-neutral message content.
  """

  defstruct [:type, :text, :data, metadata: %{}]

  @type t :: %__MODULE__{
          type: atom(),
          text: String.t() | nil,
          data: term(),
          metadata: map()
        }

  @spec text(String.t(), keyword()) :: t()
  def text(text, opts \\ []) when is_binary(text) do
    new(%{type: :text, text: text, metadata: Keyword.get(opts, :metadata, %{})})
  end

  @spec new(t() | map() | keyword()) :: t()
  def new(%__MODULE__{} = content), do: validate!(content)
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    attrs
    |> then(&struct(__MODULE__, Map.take(&1, fields())))
    |> validate!()
  end

  @spec validate!(t()) :: t()
  defp validate!(%__MODULE__{} = content) do
    unless is_atom(content.type) and not is_nil(content.type),
      do: raise(ArgumentError, "Beam content type is required")

    unless is_nil(content.text) or is_binary(content.text),
      do: raise(ArgumentError, "Beam content text must be a string")

    unless is_map(content.metadata),
      do: raise(ArgumentError, "Beam content metadata must be a map")

    content
  end

  @spec modalities(t()) :: [atom()]
  def modalities(%__MODULE__{type: :text}), do: [:text]
  def modalities(%__MODULE__{type: type}) when is_atom(type), do: [type]

  @spec fields() :: [atom()]
  defp fields do
    __MODULE__.__struct__()
    |> Map.keys()
    |> List.delete(:__struct__)
  end
end
