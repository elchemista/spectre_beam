defmodule Spectre.Beam.Plug do
  @moduledoc """
  Plug-style transformation contract for Beam endpoint pipelines.

  A plug receives a `Spectre.Beam.Pipeline` and immutable options. It may
  replace the current value, add trusted assigns, or halt with a provider-
  neutral result. Plugs never own delivery, retries, policy, or Agent state.
  """

  @callback init(keyword()) :: term()
  @callback call(Spectre.Beam.Pipeline.t(), term()) ::
              Spectre.Beam.Pipeline.t() | {:error, term()}

  @optional_callbacks init: 1
end
