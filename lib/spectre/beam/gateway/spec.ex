defmodule Spectre.Beam.Gateway.Spec do
  @moduledoc """
  Compiled, immutable configuration of one gateway.

  A specification separates what the channel boundary already understands —
  the `Spectre.Beam.Config` of endpoints, their pipelines and logistics — from
  what only a running gateway needs: which agent answers, how conversations
  are scoped, how provider events arrive, and where events are published.

  It is validated once at start-up and then read by every endpoint, outbox and
  conversation process, so a misconfiguration fails the supervisor instead of
  surfacing as a runtime error on the first message.
  """

  alias Spectre.Beam.Bus
  alias Spectre.Beam.Config
  alias Spectre.Beam.Endpoint

  @channel_keys [
    :ingress,
    :client,
    :notify,
    :agent,
    :scope,
    :session,
    :coalesce_ms,
    :idle_timeout_ms,
    :transcript_limit,
    :max_pending,
    :max_queue,
    :overflow,
    :turn_opts
  ]

  @default_coalesce_ms 0
  @default_idle_timeout_ms :timer.minutes(30)
  @default_transcript_limit 200
  @default_max_pending 50
  @default_max_queue 1_000

  defstruct [
    :name,
    :agent,
    :supervisor,
    :beam,
    :bus,
    :store,
    scope: :session,
    session?: false,
    turn_opts: [],
    channels: %{},
    control: []
  ]

  @type channel :: %{
          ingress: term(),
          client: term(),
          notify: pid() | nil,
          agent: module() | nil,
          scope: :session | :instance,
          session?: boolean(),
          coalesce_ms: non_neg_integer(),
          idle_timeout_ms: timeout(),
          transcript_limit: pos_integer(),
          max_pending: pos_integer(),
          max_queue: pos_integer(),
          overflow: :reject | :drop_oldest,
          turn_opts: keyword()
        }

  @type t :: %__MODULE__{
          name: atom(),
          agent: module() | nil,
          supervisor: GenServer.server() | nil,
          beam: Config.t(),
          bus: Bus.t(),
          store: {module(), keyword()} | nil,
          scope: :session | :instance,
          session?: boolean(),
          turn_opts: keyword(),
          channels: %{optional(term()) => channel()},
          control: keyword()
        }

  @doc """
  Compiles gateway options into a specification.

  Required: `:name` and either `:channels` or `:beam`.
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts) when is_list(opts) do
    with :ok <- keyword_options(opts),
         {:ok, name} <- fetch_name(opts),
         {:ok, scope} <- scope(Keyword.get(opts, :scope, :session)),
         {:ok, declarations} <- declarations(opts),
         {:ok, beam} <- build_config(declarations, opts),
         {:ok, channels} <- build_channels(beam, declarations, opts, scope) do
      {:ok,
       %__MODULE__{
         name: name,
         agent: Keyword.get(opts, :agent),
         supervisor: Keyword.get(opts, :supervisor),
         beam: beam,
         bus: Bus.normalize(Keyword.get(opts, :bus)),
         store: normalize_store(Keyword.get(opts, :store)),
         scope: scope,
         session?: Keyword.get(opts, :session, false) == true,
         turn_opts: Keyword.get(opts, :turn_opts, []),
         channels: channels,
         control: Keyword.get(opts, :control, [])
       }}
    end
  end

  @doc "Returns the compiled endpoint of one mounted channel."
  @spec endpoint(t(), term()) :: {:ok, Endpoint.t()} | {:error, term()}
  def endpoint(%__MODULE__{beam: beam}, id), do: Config.fetch(beam, id)

  @doc "Returns every mounted endpoint."
  @spec endpoints(t()) :: [Endpoint.t()]
  def endpoints(%__MODULE__{beam: beam}), do: beam.endpoints

  @doc "Returns the gateway settings of one mounted channel."
  @spec channel(t(), term()) :: {:ok, channel()} | {:error, term()}
  def channel(%__MODULE__{channels: channels}, id) do
    case Map.fetch(channels, id) do
      {:ok, channel} -> {:ok, channel}
      :error -> {:error, {:unknown_beam_endpoint, id}}
    end
  end

  @doc """
  Returns the agent answering on one channel.

  A channel-level `:agent` wins over the gateway default. A gateway with no
  agent at all runs in transport-only mode: inbound events are published on
  the bus and never reach Spectre.
  """
  @spec agent_for(t(), term()) :: module() | nil
  def agent_for(%__MODULE__{} = spec, id) do
    case channel(spec, id) do
      {:ok, %{agent: agent}} when not is_nil(agent) -> agent
      _other -> spec.agent
    end
  end

  @spec fetch_name(keyword()) :: {:ok, atom()} | {:error, term()}
  defp fetch_name(opts) do
    case Keyword.get(opts, :name) do
      name when is_atom(name) and not is_nil(name) -> {:ok, name}
      other -> {:error, {:invalid_beam_gateway_name, other}}
    end
  end

  @spec declarations(keyword()) :: {:ok, keyword()} | {:error, term()}
  defp declarations(opts) do
    case Keyword.get(opts, :channels, []) do
      channels when is_list(channels) ->
        if Keyword.keyword?(channels),
          do: {:ok, channels},
          else: {:error, {:invalid_beam_gateway_channels, channels}}

      channels ->
        {:error, {:invalid_beam_gateway_channels, channels}}
    end
  end

  # A gateway may reuse a configuration compiled elsewhere — an Agent mount or
  # a direct `Spectre.Beam.new/2` — instead of declaring channels inline.
  @spec build_config(keyword(), keyword()) :: {:ok, Config.t()} | {:error, term()}
  defp build_config(declarations, opts) do
    case Keyword.get(opts, :beam) do
      %Config{} = config ->
        {:ok, config}

      nil ->
        endpoints =
          Enum.map(declarations, fn {id, channel_opts} ->
            Endpoint.new(id, Keyword.drop(channel_opts, @channel_keys))
          end)

        {:ok, Config.new(endpoints, Keyword.take(opts, [:options]))}

      other ->
        {:error, {:invalid_beam_gateway_config, other}}
    end
  rescue
    exception in ArgumentError ->
      {:error, {:invalid_beam_gateway_channels, Exception.message(exception)}}
  end

  @spec build_channels(Config.t(), keyword(), keyword(), :session | :instance) ::
          {:ok, %{optional(term()) => channel()}} | {:error, term()}
  defp build_channels(beam, declarations, opts, gateway_scope) do
    endpoint_ids = beam.endpoints |> Enum.map(& &1.id) |> MapSet.new()

    with :ok <- known_declarations(declarations, endpoint_ids) do
      reduce_channels(beam.endpoints, declarations, opts, gateway_scope)
    end
  end

  @spec reduce_channels([Endpoint.t()], keyword(), keyword(), :session | :instance) ::
          {:ok, %{optional(term()) => channel()}} | {:error, term()}
  defp reduce_channels(endpoints, declarations, opts, gateway_scope) do
    Enum.reduce_while(endpoints, {:ok, %{}}, fn endpoint, {:ok, acc} ->
      channel_opts = Keyword.get(declarations, endpoint.id, [])

      case build_channel(channel_opts, opts, gateway_scope) do
        {:ok, channel} -> {:cont, {:ok, Map.put(acc, endpoint.id, channel)}}
        {:error, reason} -> {:halt, {:error, {:invalid_beam_channel, endpoint.id, reason}}}
      end
    end)
  end

  @spec build_channel(keyword(), keyword(), :session | :instance) ::
          {:ok, channel()} | {:error, term()}
  defp build_channel(channel_opts, opts, gateway_scope) do
    with {:ok, scope} <- scope(Keyword.get(channel_opts, :scope, gateway_scope)),
         {:ok, ingress} <- ingress(Keyword.get(channel_opts, :ingress)),
         {:ok, overflow} <- overflow(Keyword.get(channel_opts, :overflow, :reject)),
         {:ok, coalesce_ms} <-
           non_negative_setting(channel_opts, opts, :coalesce_ms, @default_coalesce_ms),
         {:ok, idle_timeout_ms} <- idle_timeout_setting(channel_opts, opts),
         {:ok, transcript_limit} <-
           positive_setting(channel_opts, opts, :transcript_limit, @default_transcript_limit),
         {:ok, max_pending} <-
           positive_setting(channel_opts, opts, :max_pending, @default_max_pending),
         {:ok, max_queue} <-
           positive_setting(channel_opts, opts, :max_queue, @default_max_queue),
         {:ok, turn_opts} <- turn_options(channel_opts, opts) do
      {:ok,
       %{
         ingress: ingress,
         client: Keyword.get(channel_opts, :client),
         notify: Keyword.get(channel_opts, :notify),
         agent: Keyword.get(channel_opts, :agent),
         scope: scope,
         session?:
           Keyword.get(channel_opts, :session, Keyword.get(opts, :session, false)) == true,
         coalesce_ms: coalesce_ms,
         idle_timeout_ms: idle_timeout_ms,
         transcript_limit: transcript_limit,
         max_pending: max_pending,
         max_queue: max_queue,
         overflow: overflow,
         turn_opts: turn_opts
       }}
    end
  end

  @spec known_declarations(keyword(), MapSet.t()) :: :ok | {:error, term()}
  defp known_declarations(declarations, endpoint_ids) do
    case Enum.find(declarations, fn {id, _opts} -> not MapSet.member?(endpoint_ids, id) end) do
      nil -> :ok
      {id, _opts} -> {:error, {:unknown_beam_endpoint, id}}
    end
  end

  @spec positive_setting(keyword(), keyword(), atom(), pos_integer()) ::
          {:ok, pos_integer()} | {:error, term()}
  defp positive_setting(channel_opts, opts, key, default) do
    case setting(channel_opts, opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      value -> {:error, {:invalid_beam_gateway_setting, key, value}}
    end
  end

  @spec non_negative_setting(keyword(), keyword(), atom(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  defp non_negative_setting(channel_opts, opts, key, default) do
    case setting(channel_opts, opts, key, default) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      value -> {:error, {:invalid_beam_gateway_setting, key, value}}
    end
  end

  @spec idle_timeout_setting(keyword(), keyword()) ::
          {:ok, timeout()} | {:error, term()}
  defp idle_timeout_setting(channel_opts, opts) do
    case setting(channel_opts, opts, :idle_timeout_ms, @default_idle_timeout_ms) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      :infinity -> {:ok, :infinity}
      value -> {:error, {:invalid_beam_gateway_setting, :idle_timeout_ms, value}}
    end
  end

  @spec turn_options(keyword(), keyword()) :: {:ok, keyword()} | {:error, term()}
  defp turn_options(channel_opts, opts) do
    case Keyword.get(channel_opts, :turn_opts, Keyword.get(opts, :turn_opts, [])) do
      value when is_list(value) ->
        if Keyword.keyword?(value),
          do: {:ok, value},
          else: {:error, {:invalid_beam_gateway_setting, :turn_opts, value}}

      value ->
        {:error, {:invalid_beam_gateway_setting, :turn_opts, value}}
    end
  end

  @spec setting(keyword(), keyword(), atom(), term()) :: term()
  defp setting(channel_opts, opts, key, default),
    do: Keyword.get(channel_opts, key, Keyword.get(opts, key, default))

  @spec scope(term()) :: {:ok, :session | :instance} | {:error, term()}
  defp scope(:session), do: {:ok, :session}
  defp scope(:instance), do: {:ok, :instance}
  defp scope(other), do: {:error, {:invalid_beam_gateway_scope, other}}

  @spec overflow(term()) :: {:ok, :reject | :drop_oldest} | {:error, term()}
  defp overflow(:reject), do: {:ok, :reject}
  defp overflow(:drop_oldest), do: {:ok, :drop_oldest}
  defp overflow(other), do: {:error, {:invalid_beam_outbox_overflow, other}}

  @spec ingress(term()) :: {:ok, term()} | {:error, term()}
  defp ingress(nil), do: {:ok, :none}
  defp ingress(:none), do: {:ok, :none}
  defp ingress(:subscribe), do: {:ok, :subscribe}
  defp ingress({:poll, poll_opts}) when is_list(poll_opts), do: validate_poll(poll_opts)
  defp ingress(other), do: {:error, {:invalid_beam_ingress, other}}

  @spec validate_poll(keyword()) :: {:ok, term()} | {:error, term()}
  defp validate_poll(poll_opts) do
    case Keyword.get(poll_opts, :fetch) do
      fetch when is_function(fetch, 1) ->
        {:ok, {:poll, poll_opts}}

      {module, function, args} when is_atom(module) and is_atom(function) and is_list(args) ->
        {:ok, {:poll, poll_opts}}

      other ->
        {:error, {:invalid_beam_ingress_fetch, other}}
    end
  end

  @spec normalize_store(term()) :: {module(), keyword()} | nil
  defp normalize_store(nil), do: nil
  defp normalize_store(module) when is_atom(module), do: {module, []}

  defp normalize_store({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}

  defp normalize_store(invalid),
    do: raise(ArgumentError, "invalid Beam gateway store: #{inspect(invalid)}")

  @spec keyword_options(keyword()) :: :ok | {:error, term()}
  defp keyword_options(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, {:invalid_beam_gateway_options, opts}}
  end
end
