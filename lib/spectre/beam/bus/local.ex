defmodule Spectre.Beam.Bus.Local do
  @moduledoc """
  Node-local bus backed by a duplicate-key `Registry`.

  This is the default. It adds no dependency, delivers events as plain process
  messages, and unsubscribes automatically when a subscriber dies — which is
  exactly the behaviour a LiveView or an IEx console needs.
  """

  @behaviour Spectre.Beam.Bus

  alias Spectre.Beam.Event

  @registry Spectre.Beam.Bus.Registry

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.get(opts, :name, @registry)

    Supervisor.child_spec(
      {Registry, keys: :duplicate, name: name, partitions: System.schedulers_online()},
      id: {__MODULE__, name}
    )
  end

  @impl Spectre.Beam.Bus
  def subscribe(topic, opts) do
    case Registry.register(registry(opts), topic, nil) do
      {:ok, _owner} -> :ok
      {:error, {:already_registered, _pid}} -> :ok
    end
  end

  @impl Spectre.Beam.Bus
  def unsubscribe(topic, opts) do
    Registry.unregister(registry(opts), topic)
    :ok
  end

  @impl Spectre.Beam.Bus
  def broadcast(topic, %Event{} = event, opts) do
    Registry.dispatch(registry(opts), topic, fn entries ->
      Enum.each(entries, fn {pid, _value} -> send(pid, event) end)
    end)

    :ok
  end

  @spec registry(keyword()) :: atom()
  defp registry(opts), do: Keyword.get(opts, :name, @registry)
end
