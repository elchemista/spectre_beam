defmodule Spectre.Beam.TargetResolver do
  @moduledoc """
  Resolves opaque application targets into provider-specific addresses.
  """

  @callback resolve(term(), Spectre.Beam.Endpoint.t(), Spectre.Context.t()) ::
              {:ok, term()} | {:error, term()}
end
