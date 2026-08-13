defmodule Spectre.Beam do
  @moduledoc """
  External-channel boundary for Spectre applications.

  Beam owns provider normalization, channel delivery, pipelines, and delivery
  idempotency. Spectre remains the owner of Agents, Turns, policy, identity,
  and persistence. The integration is implemented through public contracts so
  Beam does not require `:spectre` in its runtime dependency graph.
  """

  alias Spectre.Beam.Config
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Receipt
  alias Spectre.Beam.Runtime

  @version "0.3.0"
  @spectre_extension :"Elixir.Spectre.Extension"

  @doc "Returns the Beam package version."
  @spec version() :: String.t()
  def version, do: @version

  @doc """
  Publishes the Spectre Stack installable contract without taking a runtime
  dependency on Spectre.
  """
  @spec manifest() :: keyword()
  def manifest do
    [
      id: :beam,
      module: __MODULE__,
      version: @version,
      contract: 1,
      spectre: "~> 0.3.0",
      provides: [{:service, :beam}],
      agent_extensions: [Spectre.Beam.Extension],
      dsl: __MODULE__
    ]
  end

  defmacro __using__(opts) do
    quote do
      import Spectre.Beam, only: [beaming: 1, channel: 2, beam: 2]

      # Spectre is intentionally late-bound and absent from Beam's runtime deps.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      apply(
        :"Elixir.Spectre.Extension",
        :register!,
        [__MODULE__, Spectre.Beam.Extension, unquote(opts)]
      )
    end
  end

  @doc "Groups endpoint declarations on an Agent."
  defmacro beaming(do: block), do: block

  @doc "Declares one mounted external endpoint."
  defmacro channel(id, opts) do
    id = expand_value(id, __CALLER__)
    opts = expand_value(opts, __CALLER__)
    declaration = {id, opts}

    quote do
      @spectre_beam_channels unquote(Macro.escape(declaration))
    end
  end

  @doc "Stages a proactive Beam action on a Spectre Agent."
  defmacro beam(target, opts) do
    target = expand_value(target, __CALLER__)
    opts = expand_value(opts, __CALLER__)
    endpoint = Keyword.fetch!(opts, :via)
    operation = Keyword.get(opts, :operation, infer_operation(opts))

    args =
      opts
      |> Keyword.drop([:via, :operation, :policy, :reply])
      |> Map.new()
      |> Map.put(:to, target)

    action_opts =
      [args: args, mode: :write]
      |> maybe_put(:policy, Keyword.get(opts, :policy))
      |> maybe_put(:reply, Keyword.get(opts, :reply))

    quote do
      action(
        unquote(Macro.escape({:beam, endpoint, operation})),
        unquote(Macro.escape(action_opts))
      )
    end
  end

  @doc "Builds a Beam configuration directly, without an Agent or Stack."
  @spec new([Endpoint.t() | {term(), module() | keyword()}], keyword()) :: Config.t()
  def new(channels, opts \\ []) when is_list(channels) and is_list(opts) do
    endpoints =
      Enum.map(channels, fn
        %Endpoint{} = endpoint -> endpoint
        {id, adapter} when is_atom(adapter) -> Endpoint.new(id, adapter)
        {id, endpoint_opts} when is_list(endpoint_opts) -> Endpoint.new(id, endpoint_opts)
        invalid -> raise ArgumentError, "invalid Beam endpoint declaration: #{inspect(invalid)}"
      end)

    Config.new(endpoints, opts)
  end

  @doc "Returns an Agent's compiled Beam configuration."
  @spec config(module()) :: {:ok, Config.t()} | {:error, term()}
  def config(agent) when is_atom(agent) do
    with :ok <- ensure_core(@spectre_extension),
         # Spectre is intentionally late-bound and absent from Beam's runtime deps.
         # credo:disable-for-next-line Credo.Check.Refactor.Apply
         {:ok, mount} <- apply(@spectre_extension, :fetch, [agent, :beam]),
         %Config{} = config <- Map.get(mount, :compiled) do
      {:ok, config}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :invalid_beam_configuration}
    end
  end

  @doc "Normalizes one provider event through a direct config or mounted Agent."
  @spec decode(Config.t() | module(), term(), term(), keyword()) ::
          {:ok, Inbound.t()} | :ignore | {:error, term()}
  defdelegate decode(config_or_agent, endpoint, event, opts \\ []), to: Runtime

  @doc "Converts a normalized inbound into a Spectre input at runtime."
  defdelegate to_input(inbound), to: Runtime

  @doc "Builds a Spectre external identity from an authenticated inbound."
  defdelegate external_identity(inbound, opts \\ []), to: Spectre.Beam.Identity

  @doc "Resolves an authenticated inbound to its linked Spectre Instance."
  defdelegate resolve_instance(supervisor, agent, inbound, opts \\ []),
    to: Spectre.Beam.Identity

  @doc "Delivers an observable Spectre Turn reply through its inbound endpoint."
  defdelegate reply(agent, inbound, turn, opts \\ []), to: Runtime

  @doc "Runs decode, Spectre turn handling, inbound deduplication, and reply delivery."
  defdelegate handle(agent_or_session, endpoint, event, opts \\ []), to: Runtime

  @doc "Runs the identity-safe Spectre Instance path."
  defdelegate handle_instance(supervisor, agent, endpoint, event, opts \\ []), to: Runtime

  @doc "Delivers a provider-neutral outbound value through a direct Beam config."
  @spec deliver(Config.t(), term(), Outbound.t() | map() | keyword(), keyword()) ::
          {:ok, Receipt.t()} | {:error, term()}
  defdelegate deliver(config, endpoint, outbound, opts \\ []), to: Runtime

  @doc "Subscribes through a direct config or mounted Agent endpoint."
  defdelegate subscribe(config_or_agent, endpoint, opts \\ []), to: Runtime

  @doc "Unsubscribes through a direct config or mounted Agent endpoint."
  defdelegate unsubscribe(config_or_agent, endpoint, opts \\ []), to: Runtime

  @doc false
  @spec compile(keyword(), Macro.t() | nil, Macro.Env.t()) ::
          {:ok, map()} | {:error, term()}
  def compile(opts, block, %Macro.Env{} = caller) do
    channels =
      block
      |> dsl_calls()
      |> Enum.map(&compile_channel!(&1, caller))

    case duplicate_id(channels) do
      nil -> {:ok, %{options: opts, channels: channels}}
      id -> {:error, {:duplicate_beam_channel, id}}
    end
  end

  @spec compile_channel!(Macro.t(), Macro.Env.t()) :: {term(), term()}
  defp compile_channel!({:channel, _meta, [id, options]}, caller) do
    {evaluate!(id, caller), evaluate!(options, caller)}
  end

  defp compile_channel!(call, _caller) do
    raise ArgumentError, "unknown Beam Stack declaration: #{Macro.to_string(call)}"
  end

  @spec dsl_calls(Macro.t() | nil) :: [Macro.t()]
  defp dsl_calls(nil), do: []
  defp dsl_calls({:__block__, _meta, calls}), do: calls
  defp dsl_calls(one), do: [one]

  @spec evaluate!(Macro.t(), Macro.Env.t()) :: term()
  defp evaluate!(ast, caller) do
    expanded = Macro.prewalk(ast, &Macro.expand(&1, caller))
    {value, _binding} = Code.eval_quoted(expanded, [], caller)
    value
  end

  @spec duplicate_id([{term(), term()}]) :: term() | nil
  defp duplicate_id(entries) do
    entries
    |> Enum.reduce_while(MapSet.new(), fn {id, _adapter}, seen ->
      if MapSet.member?(seen, id),
        do: {:halt, id},
        else: {:cont, MapSet.put(seen, id)}
    end)
    |> case do
      %MapSet{} -> nil
      id -> id
    end
  end

  @spec ensure_core(module()) :: :ok | {:error, :spectre_not_available}
  defp ensure_core(module) do
    if Code.ensure_loaded?(module), do: :ok, else: {:error, :spectre_not_available}
  end

  @spec infer_operation(keyword()) :: atom()
  defp infer_operation(opts) do
    cond do
      Keyword.has_key?(opts, :document) -> :send_document
      Keyword.has_key?(opts, :location) -> :send_location
      Keyword.has_key?(opts, :contact) -> :send_contact
      Keyword.has_key?(opts, :event) -> :send_event
      true -> :send_text
    end
  end

  @spec maybe_put(keyword(), atom(), term()) :: keyword()
  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  @spec expand_value(Macro.t(), Macro.Env.t()) :: term()
  defp expand_value(value, caller) do
    expanded = Macro.prewalk(value, &Macro.expand(&1, caller))
    {value, _binding} = Code.eval_quoted(expanded, [], caller)
    value
  end
end
