defmodule Spectre.Beam.Receipt do
  @moduledoc """
  Normalized channel delivery receipt.
  """

  @statuses [:accepted, :sent, :delivered, :read, :failed]

  defstruct [
    :endpoint,
    :outbound_id,
    :provider_message_id,
    :status,
    :occurred_at,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          endpoint: term(),
          outbound_id: String.t(),
          provider_message_id: term(),
          status: :accepted | :sent | :delivered | :read | :failed,
          occurred_at: DateTime.t(),
          metadata: map()
        }

  @spec accepted(Spectre.Beam.Outbound.t(), keyword()) :: t()
  def accepted(outbound, opts \\ []) do
    new(%{
      endpoint: outbound.endpoint,
      outbound_id: outbound.idempotency_key,
      provider_message_id: Keyword.get(opts, :provider_message_id),
      status: :accepted,
      occurred_at: DateTime.utc_now(),
      metadata: Keyword.get(opts, :metadata, %{})
    })
  end

  @spec new(t() | map() | keyword()) :: t()
  def new(%__MODULE__{} = receipt), do: receipt |> put_occurred_at() |> validate!()
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    attrs
    |> put_occurred_at()
    |> then(&struct(__MODULE__, Map.take(&1, fields())))
    |> validate!()
  end

  @spec validate!(t()) :: t()
  defp validate!(%__MODULE__{} = receipt) do
    unless receipt.status in @statuses,
      do: raise(ArgumentError, "invalid Beam receipt status: #{inspect(receipt.status)}")

    unless is_map(receipt.metadata),
      do: raise(ArgumentError, "Beam receipt metadata must be a map")

    unless match?(%DateTime{}, receipt.occurred_at),
      do: raise(ArgumentError, "Beam receipt occurred_at must be a DateTime")

    receipt
  end

  @spec put_occurred_at(t() | map()) :: t() | map()
  defp put_occurred_at(%__MODULE__{occurred_at: nil} = receipt),
    do: %{receipt | occurred_at: DateTime.utc_now()}

  defp put_occurred_at(%__MODULE__{} = receipt), do: receipt

  defp put_occurred_at(attrs) do
    occurred_at = DateTime.utc_now()

    Map.update(attrs, :occurred_at, occurred_at, fn
      nil -> occurred_at
      occurred_at -> occurred_at
    end)
  end

  @spec fields() :: [atom()]
  defp fields do
    __MODULE__.__struct__()
    |> Map.keys()
    |> List.delete(:__struct__)
  end
end
