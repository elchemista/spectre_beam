defmodule Spectre.Beam.Throttle.Local do
  @moduledoc """
  Node-local reservation-based pacer, the default `Spectre.Beam.Throttle`.

  Every reservation books a concrete send slot, so concurrent callers are
  spaced deterministically: the endpoint schedule advances by the configured
  interval (or token-bucket debt) and the per-conversation schedule advances
  independently. State is kept in a single named GenServer; multi-node
  deployments that need cluster-wide pacing should provide their own
  `Spectre.Beam.Throttle` implementation.
  """

  @behaviour Spectre.Beam.Throttle

  use GenServer

  @default_max_wait_ms 60_000
  @conversation_prune_threshold 5_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl Spectre.Beam.Throttle
  def reserve(key, config, opts) when is_list(config) and is_list(opts) do
    server = Keyword.get(opts, :throttle_server, __MODULE__)
    GenServer.call(server, {:reserve, key, config}, :infinity)
  end

  @doc "Clears every reservation. Intended for tests."
  @spec reset(GenServer.server()) :: :ok
  def reset(server \\ __MODULE__), do: GenServer.call(server, :reset)

  @impl GenServer
  def init(:ok) do
    {:ok, initial_state()}
  end

  @impl GenServer
  def handle_call({:reserve, {endpoint_id, conversation_id}, config}, _from, state) do
    now = System.monotonic_time(:millisecond)
    state = maybe_prune(state, now)

    {global_wait, global_commit} = global_reservation(state, endpoint_id, config, now)
    {bucket_wait, bucket_commit} = bucket_reservation(state, endpoint_id, config, now)

    {conversation_wait, conversation_commit} =
      conversation_reservation(state, {endpoint_id, conversation_id}, config, now)

    wait =
      [global_wait, bucket_wait, conversation_wait]
      |> Enum.max()
      |> add_jitter(config)

    max_wait = positive_integer(Keyword.get(config, :max_wait_ms), @default_max_wait_ms)

    cond do
      wait > max_wait ->
        {:reply, {:error, {:beam_throttle_saturated, wait}}, state}

      wait > 0 and Keyword.get(config, :on_limit, :wait) == :error ->
        {:reply, {:error, {:beam_throttle_limit, wait}}, state}

      true ->
        state =
          state
          |> global_commit.(now + wait)
          |> bucket_commit.(now + wait)
          |> conversation_commit.(now + wait)

        {:reply, if(wait > 0, do: {:wait, wait}, else: :ok), state}
    end
  end

  def handle_call(:reset, _from, _state), do: {:reply, :ok, initial_state()}

  @spec initial_state() :: map()
  defp initial_state, do: %{global: %{}, buckets: %{}, conversations: %{}}

  # Fixed minimum spacing between any two sends on the endpoint.
  @spec global_reservation(map(), term(), keyword(), integer()) ::
          {non_neg_integer(), (map(), integer() -> map())}
  defp global_reservation(state, endpoint_id, config, now) do
    case positive_integer(Keyword.get(config, :min_delay_ms), 0) do
      0 ->
        {0, fn state, _slot_at -> state end}

      interval ->
        next_free = Map.get(state.global, endpoint_id, now)
        wait = max(next_free - now, 0)

        commit = fn state, slot_at ->
          %{state | global: Map.put(state.global, endpoint_id, slot_at + interval)}
        end

        {wait, commit}
    end
  end

  # Token bucket: `burst` sends may go back to back, then the rate applies.
  # When the bucket is empty the missing token is borrowed and the refill
  # timestamp moves into the future, which is what spaces later callers.
  @spec bucket_reservation(map(), term(), keyword(), integer()) ::
          {non_neg_integer(), (map(), integer() -> map())}
  defp bucket_reservation(state, endpoint_id, config, now) do
    case positive_number(Keyword.get(config, :messages_per_second)) do
      nil ->
        {0, fn state, _slot_at -> state end}

      rate ->
        capacity = config |> Keyword.get(:burst) |> positive_integer(1) |> max(1)

        {tokens, updated_at} =
          Map.get(state.buckets, endpoint_id, {capacity * 1.0, now})

        tokens = min(capacity * 1.0, tokens + max(now - updated_at, 0) * rate / 1_000)
        wait = if tokens >= 1.0, do: 0, else: ceil((1.0 - tokens) * 1_000 / rate)

        commit = fn state, slot_at ->
          %{state | buckets: Map.put(state.buckets, endpoint_id, {tokens - 1.0, slot_at})}
        end

        {wait, commit}
    end
  end

  @spec conversation_reservation(map(), term(), keyword(), integer()) ::
          {non_neg_integer(), (map(), integer() -> map())}
  defp conversation_reservation(state, key, config, now) do
    case conversation_interval(config) do
      0 ->
        {0, fn state, _slot_at -> state end}

      interval ->
        next_free = Map.get(state.conversations, key, now)
        wait = max(next_free - now, 0)

        commit = fn state, slot_at ->
          %{state | conversations: Map.put(state.conversations, key, slot_at + interval)}
        end

        {wait, commit}
    end
  end

  @spec conversation_interval(keyword()) :: non_neg_integer()
  defp conversation_interval(config) do
    per_conversation = Keyword.get(config, :per_conversation, [])

    if is_list(per_conversation) and Keyword.keyword?(per_conversation),
      do: configured_conversation_interval(per_conversation),
      else: 0
  end

  @spec configured_conversation_interval(keyword()) :: non_neg_integer()
  defp configured_conversation_interval(per_conversation) do
    min_delay = positive_integer(Keyword.get(per_conversation, :min_delay_ms), 0)
    per_minute = positive_number(Keyword.get(per_conversation, :messages_per_minute))

    cond do
      min_delay > 0 -> min_delay
      is_number(per_minute) -> ceil(60_000 / per_minute)
      true -> 0
    end
  end

  @spec add_jitter(non_neg_integer(), keyword()) :: non_neg_integer()
  defp add_jitter(wait, config) do
    case positive_integer(Keyword.get(config, :jitter_ms), 0) do
      0 -> wait
      jitter -> wait + :rand.uniform(jitter)
    end
  end

  @spec maybe_prune(map(), integer()) :: map()
  defp maybe_prune(%{conversations: conversations} = state, now)
       when map_size(conversations) > @conversation_prune_threshold do
    %{
      state
      | conversations:
          conversations
          |> Enum.filter(fn {_key, next_free} -> next_free > now end)
          |> Map.new()
    }
  end

  defp maybe_prune(state, _now), do: state

  @spec positive_integer(term(), non_neg_integer()) :: non_neg_integer()
  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  @spec positive_number(term()) :: number() | nil
  defp positive_number(value) when is_number(value) and value > 0, do: value
  defp positive_number(_value), do: nil
end
