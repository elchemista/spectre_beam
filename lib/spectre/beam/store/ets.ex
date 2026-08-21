defmodule Spectre.Beam.Store.ETS do
  @moduledoc """
  Idempotency store with bounded retention.

  `Spectre.Beam.Store` keeps every claim forever in a process map: it grows
  without limit and is lost on restart. This store keeps the same contract but
  adds the two properties a long-running gateway needs.

    * `:ttl_ms` — how long a completed claim is remembered, and therefore how
      long a provider redelivery is still recognized as a duplicate. Default
      24 hours.
    * `:claim_ttl_ms` — how long an unfinished claim fences its key. A claimer
      that crashes between `claim/2` and `complete/3` would otherwise fence
      that key permanently. Default 5 minutes.

  Reads go straight to a public ETS table, so concurrent deliveries never
  queue behind one another. Only the owning process sweeps.

      children = [
        {Spectre.Beam.Store.ETS, name: MyApp.BeamStore, ttl_ms: :timer.hours(48)}
      ]

      channel :telegram,
        adapter: Spectre.Beam.Adapters.ExGram,
        idempotency_store: {Spectre.Beam.Store.ETS, name: MyApp.BeamStore}
  """

  use GenServer

  @behaviour Spectre.Beam.IdempotencyStore

  @default_name __MODULE__
  @default_ttl_ms :timer.hours(24)
  @default_claim_ttl_ms :timer.minutes(5)
  @default_sweep_ms :timer.minutes(1)

  @type option ::
          {:name, atom()}
          | {:ttl_ms, pos_integer()}
          | {:claim_ttl_ms, pos_integer()}
          | {:sweep_interval_ms, pos_integer()}

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.get(opts, :name, @default_name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.start_link(__MODULE__, Keyword.put(opts, :name, name), name: name)
  end

  @impl Spectre.Beam.IdempotencyStore
  def claim(key, opts) do
    table = table(opts)
    now = now_ms()
    entry = {key, :in_progress, now + claim_ttl(opts)}

    if :ets.insert_new(table, entry) do
      :ok
    else
      resolve_existing(table, key, entry, now)
    end
  rescue
    ArgumentError -> {:error, {:beam_store_not_started, table(opts)}}
  end

  @impl Spectre.Beam.IdempotencyStore
  def complete(key, value, opts) do
    :ets.insert(table(opts), {key, {:completed, value}, now_ms() + ttl(opts)})
    :ok
  rescue
    ArgumentError -> {:error, {:beam_store_not_started, table(opts)}}
  end

  @impl Spectre.Beam.IdempotencyStore
  def release(key, opts) do
    :ets.delete(table(opts), key)
    :ok
  rescue
    ArgumentError -> {:error, {:beam_store_not_started, table(opts)}}
  end

  @doc "Removes every entry. Intended for tests."
  @spec reset(atom()) :: :ok
  def reset(name \\ @default_name), do: GenServer.call(name, :reset)

  @doc "Returns the number of retained entries, expired ones included."
  @spec size(atom()) :: non_neg_integer()
  def size(name \\ @default_name), do: :ets.info(name, :size) || 0

  @doc "Drops expired entries immediately instead of waiting for the sweep."
  @spec sweep(atom()) :: non_neg_integer()
  def sweep(name \\ @default_name), do: GenServer.call(name, :sweep)

  @impl GenServer
  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    _table =
      :ets.new(name, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    interval = Keyword.get(opts, :sweep_interval_ms, @default_sweep_ms)
    schedule_sweep(interval)
    {:ok, %{name: name, sweep_interval_ms: interval}}
  end

  @impl GenServer
  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(state.name)
    {:reply, :ok, state}
  end

  def handle_call(:sweep, _from, state), do: {:reply, expire(state.name), state}

  @impl GenServer
  def handle_info(:sweep, state) do
    _expired = expire(state.name)
    schedule_sweep(state.sweep_interval_ms)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # An entry that already expired is treated as absent: the claim is replaced
  # rather than reported as a duplicate, which is what lets an abandoned claim
  # heal instead of fencing its key forever.
  @spec resolve_existing(atom(), term(), tuple(), integer()) ::
          :ok | :in_progress | {:duplicate, term()}
  defp resolve_existing(table, key, entry, now) do
    case :ets.lookup(table, key) do
      [expired = {^key, _state, expires_at}] when expires_at <= now ->
        # Delete only the value we observed: another process may already have
        # renewed or completed this key between lookup and deletion. The
        # following insert_new is the single atomic winner among contenders.
        :ets.delete_object(table, expired)
        if :ets.insert_new(table, entry), do: :ok, else: :in_progress

      [{^key, :in_progress, _expires_at}] ->
        :in_progress

      [{^key, {:completed, value}, _expires_at}] ->
        {:duplicate, value}

      [] ->
        if :ets.insert_new(table, entry), do: :ok, else: :in_progress
    end
  end

  @spec expire(atom()) :: non_neg_integer()
  defp expire(table) do
    now = now_ms()

    :ets.select_delete(table, [
      {{:_, :_, :"$1"}, [{:"=<", :"$1", now}], [true]}
    ])
  end

  @spec schedule_sweep(pos_integer()) :: reference()
  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)

  @spec table(keyword()) :: atom()
  defp table(opts), do: Keyword.get(opts, :name, @default_name)

  @spec ttl(keyword()) :: pos_integer()
  defp ttl(opts), do: Keyword.get(opts, :ttl_ms, @default_ttl_ms)

  @spec claim_ttl(keyword()) :: pos_integer()
  defp claim_ttl(opts), do: Keyword.get(opts, :claim_ttl_ms, @default_claim_ttl_ms)

  @spec now_ms() :: integer()
  defp now_ms, do: System.monotonic_time(:millisecond)
end
