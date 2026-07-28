defmodule Spectre.Beam.IdempotencyStore do
  @moduledoc """
  Store contract used for inbound deduplication and outbound idempotency.
  """

  @callback claim(term(), keyword()) :: :ok | :in_progress | {:duplicate, term()}
  @callback complete(term(), term(), keyword()) :: :ok | {:error, term()}
  @callback release(term(), keyword()) :: :ok | {:error, term()}
end
