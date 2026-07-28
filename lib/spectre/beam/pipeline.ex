defmodule Spectre.Beam.Pipeline do
  @moduledoc """
  Composable provider-boundary pipeline used before and after Beam adapters.

  Supported stages are:

    * `:before_decode` for raw provider events;
    * `:after_decode` for normalized `Spectre.Beam.Inbound` values;
    * `:before_deliver` for normalized `Spectre.Beam.Outbound` values;
    * `:after_deliver` for normalized `Spectre.Beam.Receipt` values.

  Pipeline configuration contains only modules and keyword options, so it is
  safe to compile into a Stack installation. Runtime handles remain in the
  endpoint adapter options.
  """

  alias Spectre.Beam.Endpoint

  @stages [:before_decode, :after_decode, :before_deliver, :after_deliver]

  @enforce_keys [:stage, :endpoint, :value]
  defstruct [
    :stage,
    :endpoint,
    :value,
    halted?: false,
    result: nil,
    assigns: %{},
    private: %{}
  ]

  @type stage :: :before_decode | :after_decode | :before_deliver | :after_deliver
  @type spec :: module() | {module(), keyword()}

  @type t :: %__MODULE__{
          stage: stage(),
          endpoint: Endpoint.t(),
          value: term(),
          halted?: boolean(),
          result: term(),
          assigns: map(),
          private: map()
        }

  @doc """
  Runs a value through an ordered list of endpoint plugs.
  """
  @spec run(stage(), Endpoint.t(), term(), [spec()], keyword()) ::
          {:ok, term(), t()} | {:halt, term(), t()} | {:error, term()}
  def run(stage, %Endpoint{} = endpoint, value, specs, runtime_opts \\ [])
      when stage in @stages and is_list(specs) and is_list(runtime_opts) do
    pipeline = %__MODULE__{
      stage: stage,
      endpoint: endpoint,
      value: value,
      private: %{
        agent: Keyword.get(runtime_opts, :agent),
        runtime_opts: runtime_opts
      }
    }

    specs
    |> Enum.reduce_while({:ok, pipeline}, &run_plug/2)
    |> normalize_result()
  end

  @doc """
  Replaces the value passed to the next plug.
  """
  @spec put_value(t(), term()) :: t()
  def put_value(%__MODULE__{} = pipeline, value), do: %{pipeline | value: value}

  @doc """
  Adds a trusted pipeline-local assign.
  """
  @spec assign(t(), atom(), term()) :: t()
  def assign(%__MODULE__{} = pipeline, key, value) when is_atom(key) do
    %{pipeline | assigns: Map.put(pipeline.assigns, key, value)}
  end

  @doc """
  Stops the current stage with a provider-neutral result.

  `:ignore` is useful before or after decode. `{:error, reason}` can reject a
  value without raising. Other results are returned to the Beam runtime as-is.
  """
  @spec halt(t(), term()) :: t()
  def halt(%__MODULE__{} = pipeline, result \\ :ignore) do
    %{pipeline | halted?: true, result: result}
  end

  @doc """
  Validates a portable pipeline declaration.
  """
  @spec validate_specs!(term(), stage()) :: [spec()]
  def validate_specs!(specs, stage) when stage in @stages and is_list(specs) do
    Enum.map(specs, fn
      module when is_atom(module) and not is_nil(module) ->
        module

      {module, opts}
      when is_atom(module) and not is_nil(module) and is_list(opts) ->
        if Keyword.keyword?(opts) do
          {module, opts}
        else
          raise ArgumentError,
                "Beam #{stage} plug options must be a keyword list: #{inspect(opts)}"
        end

      invalid ->
        raise ArgumentError, "invalid Beam #{stage} plug: #{inspect(invalid)}"
    end)
  end

  def validate_specs!(specs, stage) do
    raise ArgumentError, "Beam #{stage} pipeline must be a list, got: #{inspect(specs)}"
  end

  @spec run_plug(spec(), {:ok, t()} | {:error, term()}) ::
          {:cont, {:ok, t()}} | {:halt, {:ok, t()} | {:error, term()}}
  defp run_plug(_spec, {:ok, %__MODULE__{halted?: true}} = halted), do: {:halt, halted}

  defp run_plug(spec, {:ok, %__MODULE__{} = pipeline}) do
    {module, opts} = normalize_spec(spec)
    run_plug(module, opts, pipeline)
  end

  @spec run_plug(module(), keyword(), t()) ::
          {:cont, {:ok, t()}} | {:halt, {:ok, t()} | {:error, term()}}
  defp run_plug(module, opts, pipeline) do
    result =
      cond do
        not Code.ensure_loaded?(module) ->
          {:error, {:beam_plug_not_loaded, pipeline.stage, module}}

        not function_exported?(module, :call, 2) ->
          {:error, {:invalid_beam_plug, pipeline.stage, module}}

        true ->
          module.call(pipeline, init(module, opts))
      end

    case result do
      %__MODULE__{} = next ->
        validate_transition(pipeline, next)

      {:error, reason} ->
        {:halt, {:error, {:beam_pipeline_rejected, pipeline.stage, module, reason}}}

      other ->
        {:halt, {:error, {:invalid_beam_plug_reply, pipeline.stage, module, other}}}
    end
  rescue
    exception ->
      {:halt, {:error, {:beam_plug_exception, pipeline.stage, module, exception.__struct__}}}
  catch
    kind, reason ->
      {:halt, {:error, {:beam_plug_failure, pipeline.stage, module, kind, reason}}}
  end

  @spec validate_transition(t(), t()) ::
          {:cont, {:ok, t()}} | {:halt, {:ok, t()} | {:error, term()}}
  defp validate_transition(previous, next) do
    cond do
      next.stage != previous.stage or next.endpoint != previous.endpoint ->
        {:halt, {:error, {:beam_plug_changed_pipeline_identity, previous.stage}}}

      next.halted? ->
        {:halt, {:ok, next}}

      true ->
        {:cont, {:ok, next}}
    end
  end

  @spec normalize_result({:ok, t()} | {:error, term()}) ::
          {:ok, term(), t()} | {:halt, term(), t()} | {:error, term()}
  defp normalize_result({:ok, %__MODULE__{halted?: true} = pipeline}),
    do: {:halt, pipeline.result, pipeline}

  defp normalize_result({:ok, %__MODULE__{} = pipeline}),
    do: {:ok, pipeline.value, pipeline}

  defp normalize_result({:error, reason}), do: {:error, reason}

  @spec normalize_spec(spec()) :: {module(), keyword()}
  defp normalize_spec(module) when is_atom(module), do: {module, []}
  defp normalize_spec({module, opts}), do: {module, opts}

  @spec init(module(), keyword()) :: term()
  defp init(module, opts) do
    if function_exported?(module, :init, 1), do: module.init(opts), else: opts
  end
end
