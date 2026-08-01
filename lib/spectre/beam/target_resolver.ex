defmodule Spectre.Beam.TargetResolver do
  @moduledoc """
  Resolves opaque application targets into provider-specific addresses.
  """

  @callback resolve(term(), Spectre.Beam.Endpoint.t(), term()) ::
              {:ok, term()} | {:error, term()}
end
