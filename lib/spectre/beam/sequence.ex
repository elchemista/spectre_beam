defmodule Spectre.Beam.Sequence do
  @moduledoc """
  Monotonic per-topic counters stamped on every published event.

  A conversation is not the only publisher of its own events: an outbox
  reports a receipt, an ingress reports an error, a console reports a status.
  A surface that reconnects still has to know what it missed, so the ordering
  cannot live in any one of those processes.

  Counters are kept in a public ETS table and advanced with
  `:ets.update_counter/4`, so stamping is atomic and never queues behind a
  process. A topic is forgotten when its conversation stops.
  """

  use GenServer

  @table __MODULE__

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :worker}
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Returns the next sequence number for a topic, starting at 1."
  @spec next(term()) :: pos_integer()
  def next(topic), do: :ets.update_counter(@table, topic, {2, 1}, {topic, 0})

  @doc "Returns the last stamped sequence number, or zero."
  @spec current(term()) :: non_neg_integer()
  def current(topic) do
    case :ets.lookup(@table, topic) do
      [{^topic, value}] -> value
      [] -> 0
    end
  end

  @doc "Drops a topic's counter once its conversation is gone."
  @spec forget(term()) :: :ok
  def forget(topic) do
    :ets.delete(@table, topic)
    :ok
  end

  @impl GenServer
  def init(_opts) do
    _table = :ets.new(@table, [:set, :public, :named_table, write_concurrency: true])
    {:ok, %{}}
  end
end
