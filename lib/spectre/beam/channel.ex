defmodule Spectre.Beam.Channel do
  @moduledoc """
  Provider-neutral contract implemented by Beam channel adapters.

  An adapter owns only the provider boundary: it normalizes inbound events,
  delivers normalized outbound values, and may expose subscription lifecycle
  callbacks. Authentication, filtering, enrichment, observability, and other
  cross-provider changes belong in `Spectre.Beam.Plug` pipelines.

  Provider libraries are deliberately optional dependencies. An adapter can
  invoke them dynamically and return a stable Beam error when they are absent.
  """

  @callback capabilities(keyword()) :: MapSet.t(atom()) | [atom()]
  @callback decode(term(), keyword()) ::
              {:ok, Spectre.Beam.Inbound.t() | map()} | :ignore | {:error, term()}
  @callback deliver(Spectre.Beam.Outbound.t(), keyword()) ::
              {:ok, Spectre.Beam.Receipt.t() | map()} | {:error, term()}
  @callback acknowledge(term(), keyword()) :: :ok | {:error, term()}
  @callback normalize_receipt(term(), keyword()) ::
              {:ok, Spectre.Beam.Receipt.t() | map()} | :ignore | {:error, term()}
  @callback subscribe(keyword()) :: :ok | {:error, term()}
  @callback unsubscribe(keyword()) :: :ok | {:error, term()}
  @callback typing(to :: term(), composing? :: boolean(), keyword()) :: :ok | {:error, term()}

  @optional_callbacks capabilities: 1,
                      acknowledge: 2,
                      normalize_receipt: 2,
                      subscribe: 1,
                      unsubscribe: 1,
                      typing: 3
end
