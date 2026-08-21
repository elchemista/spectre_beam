defmodule Spectre.Beam.Gateway.Holder do
  @moduledoc """
  Publishes a gateway's compiled specification to every process that needs it.

  The specification is immutable and read on nearly every message, so it is
  registered as the value of a `Registry` entry rather than fetched from a
  process. Lookups are direct ETS reads; the entry disappears when the gateway
  stops, which is what a supervisor restart should look like.
  """

  use GenServer

  alias Spectre.Beam.Gateway.Spec

  @registry Spectre.Beam.Registry

  @doc false
  @spec child_spec(Spec.t()) :: Supervisor.child_spec()
  def child_spec(%Spec{} = spec) do
    %{id: {__MODULE__, spec.name}, start: {__MODULE__, :start_link, [spec]}, type: :worker}
  end

  @spec start_link(Spec.t()) :: GenServer.on_start()
  def start_link(%Spec{} = spec), do: GenServer.start_link(__MODULE__, spec)

  @doc "Returns the compiled specification of a running gateway."
  @spec fetch(atom()) :: {:ok, Spec.t()} | {:error, :not_found}
  def fetch(name) when is_atom(name) do
    case Registry.lookup(@registry, {:gateway, name}) do
      [{_pid, %Spec{} = spec}] -> {:ok, spec}
      [] -> {:error, :not_found}
    end
  end

  @doc "Returns every running gateway name on this node."
  @spec list() :: [atom()]
  def list do
    Registry.select(@registry, [
      {{{:gateway, :"$1"}, :_, :_}, [], [:"$1"]}
    ])
  end

  @impl GenServer
  def init(spec) do
    case Registry.register(@registry, {:gateway, spec.name}, spec) do
      {:ok, _owner} -> {:ok, spec}
      {:error, {:already_registered, _pid}} -> {:stop, {:already_started, spec.name}}
    end
  end
end
