defmodule Spectre.Beam.Store do
  @moduledoc """
  Node-local default idempotency store.

  Production applications can configure a database-backed implementation on
  each endpoint. This store provides deterministic single-node behavior and is
  supervised by the Beam application.
  """

  use GenServer

  @behaviour Spectre.Beam.IdempotencyStore

  @name __MODULE__

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: @name)

  @impl Spectre.Beam.IdempotencyStore
  def claim(key, _opts), do: GenServer.call(@name, {:claim, key})

  @impl Spectre.Beam.IdempotencyStore
  def complete(key, value, _opts), do: GenServer.call(@name, {:complete, key, value})

  @impl Spectre.Beam.IdempotencyStore
  def release(key, _opts), do: GenServer.call(@name, {:release, key})

  @doc false
  def reset, do: GenServer.call(@name, :reset)

  @impl GenServer
  def init(state), do: {:ok, state}

  @impl GenServer
  def handle_call({:claim, key}, _from, state) do
    case Map.get(state, key) do
      nil -> {:reply, :ok, Map.put(state, key, :in_progress)}
      :in_progress -> {:reply, :in_progress, state}
      {:completed, value} -> {:reply, {:duplicate, value}, state}
    end
  end

  def handle_call({:complete, key, value}, _from, state),
    do: {:reply, :ok, Map.put(state, key, {:completed, value})}

  def handle_call({:release, key}, _from, state),
    do: {:reply, :ok, Map.delete(state, key)}

  def handle_call(:reset, _from, _state), do: {:reply, :ok, %{}}
end
