defmodule Spectre.Beam.Endpoint do
  @moduledoc """
  One configured external channel endpoint.
  """

  alias Spectre.Beam.Pipeline

  @known_options [
    :type,
    :adapter,
    :capabilities,
    :planner_exposure,
    :target_resolver,
    :deduplicator,
    :idempotency_store,
    :max_payload_bytes,
    :pipelines,
    :before_decode,
    :inbound_pipeline,
    :outbound_pipeline,
    :receipt_pipeline
  ]

  @pipeline_stages [:before_decode, :after_decode, :before_deliver, :after_deliver]

  defstruct [
    :id,
    :type,
    :adapter,
    :target_resolver,
    capabilities: nil,
    planner_exposure: :none,
    pipelines: %{},
    opts: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          id: term(),
          type: atom() | String.t(),
          adapter: module(),
          target_resolver: term(),
          capabilities: MapSet.t(atom()) | nil,
          planner_exposure: :none | :all | [atom()],
          pipelines: %{optional(Spectre.Beam.Pipeline.stage()) => [Spectre.Beam.Pipeline.spec()]},
          opts: keyword(),
          metadata: map()
        }

  @spec new(term(), keyword() | module()) :: t()
  def new(id, adapter) when is_atom(adapter), do: new(id, adapter: adapter)

  def new(id, opts) when is_list(opts) do
    unless Keyword.keyword?(opts),
      do: raise(ArgumentError, "Beam channel options must be a keyword list")

    endpoint = %__MODULE__{
      id: id,
      type: Keyword.get(opts, :type, id),
      adapter: Keyword.fetch!(opts, :adapter),
      target_resolver: Keyword.get(opts, :target_resolver),
      capabilities: normalize_capabilities(Keyword.get(opts, :capabilities)),
      planner_exposure: Keyword.get(opts, :planner_exposure, :none),
      pipelines: normalize_pipelines(opts),
      opts: Keyword.drop(opts, @known_options),
      metadata: %{
        deduplicator: Keyword.get(opts, :deduplicator),
        idempotency_store: Keyword.get(opts, :idempotency_store),
        max_payload_bytes: Keyword.get(opts, :max_payload_bytes)
      }
    }

    validate!(endpoint)
    endpoint
  end

  @doc """
  Returns actual adapter capabilities, preferring an explicit declaration.
  """
  @spec capabilities(t()) :: {:ok, MapSet.t(atom())} | {:error, term()}
  def capabilities(%__MODULE__{capabilities: %MapSet{} = capabilities}),
    do: {:ok, capabilities}

  def capabilities(%__MODULE__{} = endpoint) do
    cond do
      not Code.ensure_loaded?(endpoint.adapter) ->
        {:error, {:beam_adapter_not_loaded, endpoint.id, endpoint.adapter}}

      function_exported?(endpoint.adapter, :capabilities, 1) ->
        normalize_capability_reply(endpoint.adapter.capabilities(endpoint.opts), endpoint)

      true ->
        {:ok, MapSet.new([:text])}
    end
  rescue
    exception -> {:error, {:beam_capabilities_exception, endpoint.id, exception.__struct__}}
  catch
    kind, reason -> {:error, {:beam_capabilities_failure, endpoint.id, kind, reason}}
  end

  @spec adapter_opts(t(), keyword()) :: keyword()
  def adapter_opts(%__MODULE__{} = endpoint, runtime_opts \\ []) do
    Keyword.merge(endpoint.opts, Keyword.get(runtime_opts, :adapter_opts, []))
  end

  @doc """
  Returns the ordered plugs configured for a boundary stage.
  """
  @spec pipeline(t(), Spectre.Beam.Pipeline.stage()) :: [Spectre.Beam.Pipeline.spec()]
  def pipeline(%__MODULE__{pipelines: pipelines}, stage) when stage in @pipeline_stages do
    Map.get(pipelines, stage, [])
  end

  @spec normalize_capability_reply(term(), t()) ::
          {:ok, MapSet.t(atom())} | {:error, term()}
  defp normalize_capability_reply(%MapSet{} = capabilities, _endpoint),
    do: {:ok, capabilities}

  defp normalize_capability_reply(capabilities, _endpoint) when is_list(capabilities),
    do: {:ok, MapSet.new(capabilities)}

  defp normalize_capability_reply(other, endpoint),
    do: {:error, {:invalid_beam_capabilities, endpoint.id, other}}

  @spec normalize_capabilities(term()) :: MapSet.t(atom()) | nil
  defp normalize_capabilities(nil), do: nil
  defp normalize_capabilities(%MapSet{} = capabilities), do: capabilities

  defp normalize_capabilities(capabilities) when is_list(capabilities),
    do: MapSet.new(capabilities)

  defp normalize_capabilities(other),
    do: raise(ArgumentError, "invalid Beam capabilities: #{inspect(other)}")

  @spec normalize_pipelines(keyword()) :: map()
  defp normalize_pipelines(opts) do
    configured =
      opts
      |> Keyword.get(:pipelines, [])
      |> normalize_pipeline_container()
      |> put_pipeline(:before_decode, Keyword.get(opts, :before_decode))
      |> put_pipeline(:after_decode, Keyword.get(opts, :inbound_pipeline))
      |> put_pipeline(:before_deliver, Keyword.get(opts, :outbound_pipeline))
      |> put_pipeline(:after_deliver, Keyword.get(opts, :receipt_pipeline))

    Map.new(@pipeline_stages, fn stage ->
      {stage,
       configured
       |> Map.get(stage, [])
       |> Pipeline.validate_specs!(stage)}
    end)
  end

  @spec normalize_pipeline_container(term()) :: map()
  defp normalize_pipeline_container(pipelines) when is_list(pipelines) do
    if Keyword.keyword?(pipelines) do
      pipelines
      |> Map.new()
      |> validate_pipeline_stages!()
    else
      raise ArgumentError, "Beam pipelines must be a keyword list or map"
    end
  end

  defp normalize_pipeline_container(pipelines) when is_map(pipelines),
    do: validate_pipeline_stages!(pipelines)

  defp normalize_pipeline_container(pipelines),
    do:
      raise(ArgumentError, "Beam pipelines must be a keyword list or map: #{inspect(pipelines)}")

  @spec put_pipeline(map(), atom(), term()) :: map()
  defp put_pipeline(pipelines, _stage, nil), do: pipelines
  defp put_pipeline(pipelines, stage, specs), do: Map.put(pipelines, stage, specs)

  @spec validate_pipeline_stages!(map()) :: map()
  defp validate_pipeline_stages!(pipelines) do
    case Map.keys(pipelines) -- @pipeline_stages do
      [] ->
        pipelines

      unknown ->
        raise ArgumentError, "unknown Beam pipeline stages: #{inspect(unknown)}"
    end
  end

  @spec validate!(t()) :: :ok
  defp validate!(%__MODULE__{} = endpoint) do
    unless valid_id?(endpoint.id), do: raise(ArgumentError, "invalid Beam endpoint id")

    unless is_atom(endpoint.adapter) and not is_nil(endpoint.adapter),
      do: raise(ArgumentError, "Beam endpoint adapter must be a module")

    unless endpoint.planner_exposure == :none or endpoint.planner_exposure == :all or
             (is_list(endpoint.planner_exposure) and
                Enum.all?(endpoint.planner_exposure, &is_atom/1)) do
      raise ArgumentError, "invalid Beam planner exposure"
    end

    :ok
  end

  @spec valid_id?(term()) :: boolean()
  defp valid_id?(id), do: (is_atom(id) and not is_nil(id)) or (is_binary(id) and id != "")
end
