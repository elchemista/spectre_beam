defmodule Spectre.Beam.Logistics do
  @moduledoc """
  Delivery logistics applied by the runtime around each adapter send.

  Channel plumbing that every conversational provider needs — pacing, a typing
  indicator, a human reply delay, and bounded retries — is configured per
  endpoint (with per-call overrides) and executed while the outbound
  idempotency claim is held, so a concurrent duplicate send still observes
  `:in_progress` during the whole sequence:

      channel :whatsapp,
        adapter: Spectre.Beam.Adapters.ExWapp,
        typing: true,
        reply_delay_ms: 2_000,
        throttle: [messages_per_second: 2.0, burst: 4],
        retry: [max_attempts: 3, base_delay_ms: 250]

  Option semantics:

    * `typing:` — `true` calls the adapter's optional `typing/3` callback right
      before the delay/send so the pause reads as composing. Best effort: a
      missing callback or provider error never fails the delivery.
    * `reply_delay_ms:` — `non_neg_integer` or `{min, max}` for a randomized
      pause between the typing signal and the provider call.
    * `throttle:` — see `Spectre.Beam.Throttle`. `{module, config}` swaps the
      strategy; a rejected reservation aborts with
      `{:error, {:beam_throttled, endpoint_id, reason}}` and releases the
      claim, so the caller may retry later.
    * `retry:` — `[max_attempts:, base_delay_ms:, max_delay_ms:, jitter_ms:,
      retry_on:]`. Only plain `{:error, reason}` adapter replies are retried
      (exponential backoff); `{:error, {:ambiguous, _}}` is never retried
      because the provider may already have accepted the message. `retry_on:`
      narrows retryable reasons with a `(reason -> boolean)` function.
  """

  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Outbound

  @option_keys [:typing, :reply_delay_ms, :retry, :throttle]

  @default_base_delay_ms 250
  @default_max_delay_ms 5_000

  @doc "Configuration keys owned by delivery logistics."
  @spec option_keys() :: [atom()]
  def option_keys, do: @option_keys

  @doc """
  Runs pacing, typing, and the reply delay for one outbound delivery.
  """
  @spec before_deliver(Endpoint.t(), Outbound.t(), keyword()) :: :ok | {:error, term()}
  def before_deliver(%Endpoint{} = endpoint, %Outbound{} = outbound, opts) do
    with :ok <- throttle(endpoint, outbound, opts) do
      typing(endpoint, outbound, opts)
      delay(endpoint, opts)
      :ok
    end
  end

  @doc """
  Invokes `deliver_fun` with bounded retries according to the retry policy.

  `deliver_fun` receives the attempt number (starting at 1). Ambiguous errors
  and exhausted policies return the last error unchanged.
  """
  @spec deliver_with_retry(Endpoint.t(), keyword(), (pos_integer() -> result)) :: result
        when result: {:ok, term()} | {:error, term()}
  def deliver_with_retry(%Endpoint{} = endpoint, opts, deliver_fun)
      when is_function(deliver_fun, 1) do
    policy = keyword_option(endpoint, opts, :retry)
    max_attempts = positive_integer(Keyword.get(policy, :max_attempts), 1)

    attempt_deliver(deliver_fun, policy, 1, max_attempts)
  end

  @spec attempt_deliver((pos_integer() -> term()), keyword(), pos_integer(), pos_integer()) ::
          {:ok, term()} | {:error, term()}
  defp attempt_deliver(deliver_fun, policy, attempt, max_attempts) do
    case deliver_fun.(attempt) do
      {:error, reason} = error when attempt < max_attempts ->
        if retryable?(reason, policy) do
          Process.sleep(backoff(policy, attempt))
          attempt_deliver(deliver_fun, policy, attempt + 1, max_attempts)
        else
          error
        end

      result ->
        result
    end
  end

  @spec retryable?(term(), keyword()) :: boolean()
  defp retryable?({:ambiguous, _reason}, _policy), do: false

  defp retryable?(reason, policy) do
    case Keyword.get(policy, :retry_on) do
      filter when is_function(filter, 1) -> filter.(reason) == true
      _no_filter -> true
    end
  end

  @spec backoff(keyword(), pos_integer()) :: non_neg_integer()
  defp backoff(policy, attempt) do
    base = positive_integer(Keyword.get(policy, :base_delay_ms), @default_base_delay_ms)
    max_delay = positive_integer(Keyword.get(policy, :max_delay_ms), @default_max_delay_ms)

    delay = min(base * Integer.pow(2, attempt - 1), max_delay)

    case positive_integer(Keyword.get(policy, :jitter_ms), 0) do
      0 -> delay
      jitter -> delay + :rand.uniform(jitter)
    end
  end

  @spec throttle(Endpoint.t(), Outbound.t(), keyword()) :: :ok | {:error, term()}
  defp throttle(endpoint, outbound, opts) do
    case option(endpoint, opts, :throttle) do
      disabled when disabled in [nil, false] ->
        :ok

      configured ->
        {module, config} = throttle_strategy(configured)
        key = {endpoint.id, outbound.conversation_id}

        case reserve(module, key, config, opts) do
          :ok ->
            :ok

          {:wait, wait_ms} when is_integer(wait_ms) and wait_ms > 0 ->
            Process.sleep(wait_ms)
            :ok

          {:error, reason} ->
            {:error, {:beam_throttled, endpoint.id, reason}}

          other ->
            {:error, {:beam_throttled, endpoint.id, {:invalid_throttle_reply, other}}}
        end
    end
  end

  @spec throttle_strategy(term()) :: {module(), keyword()}
  defp throttle_strategy({module, config}) when is_atom(module) and is_list(config),
    do: {module, config}

  defp throttle_strategy(config) when is_list(config), do: {Spectre.Beam.Throttle.Local, config}
  defp throttle_strategy(true), do: {Spectre.Beam.Throttle.Local, []}
  defp throttle_strategy(other), do: {__MODULE__.InvalidThrottle, [configured: other]}

  @spec reserve(module(), Spectre.Beam.Throttle.key(), keyword(), keyword()) :: term()
  defp reserve(module, key, config, opts) do
    if Code.ensure_loaded?(module) and function_exported?(module, :reserve, 3) do
      module.reserve(key, config, opts)
    else
      {:error, {:invalid_beam_throttle, module}}
    end
  rescue
    exception -> {:error, {:beam_throttle_exception, module, exception.__struct__}}
  catch
    kind, reason -> {:error, {:beam_throttle_failure, module, kind, reason}}
  end

  # Typing is a courtesy signal: any adapter or provider failure is ignored so
  # it can never turn a deliverable message into a failed one.
  @spec typing(Endpoint.t(), Outbound.t(), keyword()) :: :ok
  defp typing(endpoint, outbound, opts) do
    with true <- option(endpoint, opts, :typing) == true,
         adapter = endpoint.adapter,
         true <- Code.ensure_loaded?(adapter) and function_exported?(adapter, :typing, 3) do
      _result = adapter.typing(outbound.to, true, Endpoint.adapter_opts(endpoint, opts))
      :ok
    else
      _disabled_or_unsupported -> :ok
    end
  rescue
    _exception -> :ok
  catch
    _kind, _reason -> :ok
  end

  @spec delay(Endpoint.t(), keyword()) :: :ok
  defp delay(endpoint, opts) do
    case option(endpoint, opts, :reply_delay_ms) do
      delay_ms when is_integer(delay_ms) and delay_ms > 0 ->
        Process.sleep(delay_ms)

      {min_ms, max_ms}
      when is_integer(min_ms) and is_integer(max_ms) and min_ms >= 0 and max_ms > min_ms ->
        Process.sleep(min_ms + :rand.uniform(max_ms - min_ms))

      _disabled ->
        :ok
    end

    :ok
  end

  @spec option(Endpoint.t(), keyword(), atom()) :: term()
  defp option(%Endpoint{} = endpoint, opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> value
      :error -> Map.get(endpoint.metadata, key)
    end
  end

  @spec keyword_option(Endpoint.t(), keyword(), atom()) :: keyword()
  defp keyword_option(endpoint, opts, key) do
    case option(endpoint, opts, key) do
      value when is_list(value) -> if Keyword.keyword?(value), do: value, else: []
      _other -> []
    end
  end

  @spec positive_integer(term(), non_neg_integer()) :: non_neg_integer()
  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default
end
