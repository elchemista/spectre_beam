defmodule Spectre.Beam.Extension do
  @moduledoc """
  Late-bound Spectre Agent extension used by `Spectre.Beam`.

  The callbacks return ordinary maps, tuples, and Beam structs, allowing this
  module to compile without Spectre while remaining compatible with Spectre's
  public extension contract when both libraries are installed.
  """

  alias Spectre.Beam.ActionProvider
  alias Spectre.Beam.Adapters.Local
  alias Spectre.Beam.Config
  alias Spectre.Beam.Endpoint

  @provider_mount :"Elixir.Spectre.Action.Provider.Mount"

  @endpoint_default_keys [
    :pipelines,
    :before_decode,
    :inbound_pipeline,
    :outbound_pipeline,
    :receipt_pipeline,
    :deduplicator,
    :idempotency_store,
    :max_payload_bytes,
    :typing,
    :reply_delay_ms,
    :retry,
    :throttle
  ]

  def id, do: :beam
  def api_version, do: 1

  def setup(owner, _opts) do
    Module.register_attribute(owner, :spectre_beam_channels, accumulate: true, persist: false)
    :ok
  end

  def compile(owner, opts) do
    stack? = Keyword.has_key?(opts, :stack_config)

    {declarations, options} =
      case Keyword.fetch(opts, :stack_config) do
        {:ok, %{channels: channels} = config} ->
          {channels, Map.get(config, :options, [])}

        :error ->
          declarations =
            owner
            |> Module.get_attribute(:spectre_beam_channels)
            |> reverse()

          {declarations, opts}
      end

    declarations = add_local_channel(declarations, opts, stack?)

    with :ok <- unique(declarations),
         {:ok, endpoints} <- endpoints(declarations, options) do
      {:ok, Config.new(endpoints, options)}
    end
  end

  # Direct `use Spectre.Beam` is immediately useful from IEx and LiveView.
  # Stack installations remain fully declarative and receive only the
  # endpoints explicitly present in their installation block.
  @spec add_local_channel([{term(), keyword() | module()}], keyword(), boolean()) ::
          [{term(), keyword() | module()}]
  defp add_local_channel(declarations, _opts, true), do: declarations

  defp add_local_channel(declarations, opts, false) do
    if Keyword.get(opts, :local, true) == false or local_channel?(declarations) do
      declarations
    else
      declarations ++ [{local_id(declarations), [type: :local, adapter: Local]}]
    end
  end

  @spec local_channel?([{term(), keyword() | module()}]) :: boolean()
  defp local_channel?(declarations) do
    Enum.any?(declarations, fn
      {_id, Local} ->
        true

      {_id, channel_opts} when is_list(channel_opts) ->
        Keyword.get(channel_opts, :adapter) == Local

      _other ->
        false
    end)
  end

  @spec local_id([{term(), term()}]) :: atom()
  defp local_id(declarations) do
    ids = MapSet.new(declarations, &elem(&1, 0))
    if MapSet.member?(ids, :local), do: :beam_local, else: :local
  end

  def agent_config(%Config{} = config), do: [beam: config]

  def expand_handler({:beam, _meta, [target, opts]}, caller, _mount_opts) do
    target = expand_value(target, caller)
    opts = expand_value(opts, caller)

    if is_list(opts) and Keyword.keyword?(opts) do
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

      {:ok,
       quote do
         action(
           unquote(Macro.escape({:beam, endpoint, operation})),
           unquote(Macro.escape(action_opts))
         )
       end}
    else
      {:error, {:invalid_beam_handler_options, opts}}
    end
  end

  def expand_handler(_handler, _caller, _mount_opts), do: :ignore

  def flow_constraints(opts, %Config{} = config) do
    case Keyword.pop(opts, :beam) do
      {nil, remaining} ->
        {[], remaining}

      {mounts, remaining} ->
        mounts = List.wrap(mounts)

        cond do
          mounts == [] ->
            {:error, :beam_flow_requires_endpoint}

          unknown = Enum.find(mounts, &(not Map.has_key?(config.by_id, &1))) ->
            {:error, {:unknown_beam_endpoint, unknown}}

          true ->
            {[%{namespace: :beam, kind: :source, values: mounts, mode: :any}], remaining}
        end
    end
  end

  def action_providers(%Config{} = config) do
    Enum.map(config.endpoints, fn endpoint ->
      if Code.ensure_loaded?(@provider_mount) and function_exported?(@provider_mount, :new, 3) do
        # Spectre is intentionally late-bound and absent from Beam's runtime deps.
        # credo:disable-for-next-line Credo.Check.Refactor.Apply
        apply(@provider_mount, :new, [
          {:beam, endpoint.id},
          ActionProvider,
          [endpoint: endpoint]
        ])
      else
        {{:beam, endpoint.id}, ActionProvider, endpoint: endpoint}
      end
    end)
  end

  @spec endpoints([{term(), keyword() | module()}], keyword()) ::
          {:ok, [Endpoint.t()]} | {:error, term()}
  defp endpoints(declarations, options) do
    defaults = Keyword.take(options, @endpoint_default_keys)

    endpoints =
      Enum.map(declarations, fn
        {id, adapter} when is_atom(adapter) ->
          Endpoint.new(id, Keyword.put(defaults, :adapter, adapter))

        {id, opts} when is_list(opts) ->
          Endpoint.new(id, Keyword.merge(defaults, opts))
      end)

    {:ok, endpoints}
  rescue
    exception -> {:error, {:invalid_beam_endpoint, Exception.message(exception)}}
  end

  @spec unique([{term(), term()}]) :: :ok | {:error, term()}
  defp unique(declarations) do
    case declarations |> Enum.map(&elem(&1, 0)) |> duplicate() do
      nil -> :ok
      id -> {:error, {:duplicate_beam_channel, id}}
    end
  end

  @spec duplicate([term()]) :: term() | nil
  defp duplicate(values) do
    values
    |> Enum.reduce_while(MapSet.new(), fn value, seen ->
      if MapSet.member?(seen, value),
        do: {:halt, value},
        else: {:cont, MapSet.put(seen, value)}
    end)
    |> case do
      %MapSet{} -> nil
      duplicate -> duplicate
    end
  end

  @spec reverse(list() | nil) :: list()
  defp reverse(nil), do: []
  defp reverse(values), do: Enum.reverse(values)

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
