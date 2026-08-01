defmodule Spectre.Beam.Config do
  @moduledoc """
  Immutable set of standalone channel endpoints.
  """

  defstruct endpoints: [], by_id: %{}, options: []

  @type t :: %__MODULE__{
          endpoints: [Spectre.Beam.Endpoint.t()],
          by_id: %{optional(term()) => Spectre.Beam.Endpoint.t()},
          options: keyword()
        }

  @spec new([Spectre.Beam.Endpoint.t()], keyword()) :: t()
  def new(endpoints, options \\ []) when is_list(endpoints) and is_list(options) do
    unless Keyword.keyword?(options),
      do: raise(ArgumentError, "Beam configuration options must be a keyword list")

    case duplicate_id(endpoints) do
      nil -> :ok
      id -> raise ArgumentError, "duplicate Beam endpoint: #{inspect(id)}"
    end

    %__MODULE__{
      endpoints: endpoints,
      by_id: Map.new(endpoints, &{&1.id, &1}),
      options: options
    }
  end

  @spec fetch(t(), term()) ::
          {:ok, Spectre.Beam.Endpoint.t()} | {:error, {:unknown_beam_endpoint, term()}}
  def fetch(%__MODULE__{by_id: endpoints}, id) do
    case Map.fetch(endpoints, id) do
      {:ok, endpoint} -> {:ok, endpoint}
      :error -> {:error, {:unknown_beam_endpoint, id}}
    end
  end

  defp duplicate_id(endpoints) do
    endpoints
    |> Enum.map(& &1.id)
    |> Enum.reduce_while(MapSet.new(), fn id, seen ->
      if MapSet.member?(seen, id),
        do: {:halt, id},
        else: {:cont, MapSet.put(seen, id)}
    end)
    |> case do
      %MapSet{} -> nil
      id -> id
    end
  end
end
