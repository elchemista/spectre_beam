defmodule Spectre.Beam.Throttle do
  @moduledoc """
  Behaviour for outbound pacing strategies.

  The runtime asks the configured throttle to reserve a delivery slot right
  before invoking the channel adapter, while the outbound idempotency claim is
  already held. A reservation either proceeds immediately, tells the caller how
  long to wait for its slot, or rejects the send without consuming a slot.

  Configure per endpoint (or per call) with:

      channel :whatsapp,
        adapter: Spectre.Beam.Adapters.ExWapp,
        throttle: [messages_per_second: 2.0, burst: 4, min_delay_ms: 400,
                   per_conversation: [messages_per_minute: 12],
                   jitter_ms: 250, max_wait_ms: 60_000, on_limit: :wait]

  A `{module, config}` tuple swaps the strategy for any module implementing
  this behaviour; `false` (or omitting the option) disables pacing.

  Recognized configuration for the bundled `Spectre.Beam.Throttle.Local`:

    * `:min_delay_ms` — minimum interval between sends on the endpoint.
    * `:messages_per_second` + `:burst` — endpoint token bucket; `burst`
      (default 1) sends may proceed back to back before the rate applies.
    * `:per_conversation` — `[min_delay_ms: n]` or `[messages_per_minute: n]`,
      spacing applied per conversation.
    * `:jitter_ms` — random extra delay added to every reserved slot.
    * `:max_wait_ms` — reject reservations that would wait longer (default
      `60_000`), returned as `{:error, {:beam_throttle_saturated, wait_ms}}`.
    * `:on_limit` — `:wait` (default) sleeps until the slot; `:error` rejects
      whenever any wait would be required.
  """

  @typedoc "Pacing scope: the endpoint and the conversation being addressed."
  @type key :: {endpoint_id :: term(), conversation_id :: term()}

  @callback reserve(key(), config :: keyword(), opts :: keyword()) ::
              :ok | {:wait, non_neg_integer()} | {:error, term()}
end
