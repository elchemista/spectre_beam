defmodule Spectre.Beam.Config do
  @moduledoc """
  Immutable set of endpoints compiled into an Agent extension mount.
  """

  defstruct endpoints: [], by_id: %{}, options: []

  @type t :: %__MODULE__{
          endpoints: [Spectre.Beam.Endpoint.t()],
          by_id: %{optional(term()) => Spectre.Beam.Endpoint.t()},
          options: keyword()
        }

  @spec new([Spectre.Beam.Endpoint.t()], keyword()) :: t()
  def new(endpoints, options \\ []) when is_list(endpoints) and is_list(options) do
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
end
