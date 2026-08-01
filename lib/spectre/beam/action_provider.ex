defmodule Spectre.Beam.ActionProvider do
  @moduledoc """
  Spectre action-provider callbacks for proactive Beam delivery.

  The module implements the public provider callback shape without a compile
  dependency on Spectre. Spectre invokes it after both libraries are loaded.
  """

  alias Spectre.Beam.Content
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Runtime

  @spec_module :"Elixir.Spectre.Action.Spec"

  @capability_operations %{
    text: :send_text,
    document: :send_document,
    location: :send_location,
    contact: :send_contact,
    event: :send_event
  }

  @operation_content %{
    send_document: :document,
    send_location: :location,
    send_contact: :contact,
    send_event: :event
  }

  def actions(opts) do
    endpoint = Keyword.fetch!(opts, :endpoint)

    with {:ok, capabilities} <- Endpoint.capabilities(endpoint) do
      capabilities
      |> Enum.flat_map(&capability_spec(&1, endpoint))
      |> Enum.sort_by(&Map.fetch!(&1, :name))
    end
  end

  def execute(action, ctx, opts) when is_map(action) and is_map(ctx) do
    endpoint = Keyword.fetch!(opts, :endpoint)
    context_opts = Map.get(ctx, :opts, [])

    with :ok <- validate_action(action, endpoint),
         {:ok, target} <- resolve_target(arg(action.args, :to), endpoint, ctx),
         {:ok, content} <- content(action.name, action.args, ctx),
         idempotency_key when is_binary(idempotency_key) <-
           Keyword.get(context_opts, :idempotency_key),
         outbound <-
           Outbound.new(%{
             endpoint: endpoint.id,
             conversation_id: arg(action.args, :conversation_id, target),
             to: target,
             reply_to: arg(action.args, :reply_to),
             content: content,
             idempotency_key: idempotency_key,
             metadata: %{kind: :proactive}
           }) do
      Runtime.deliver(endpoint, outbound, Keyword.put(context_opts, :agent, Map.get(ctx, :agent)))
    else
      nil -> {:error, :missing_beam_idempotency_key}
      {:error, _reason} = error -> error
    end
  end

  def schema_hash(action, opts) when is_map(action) do
    endpoint = Keyword.fetch!(opts, :endpoint)

    case actions(opts) do
      specs when is_list(specs) ->
        find_schema_hash(specs, action, endpoint)

      {:error, _reason} ->
        nil
    end
  end

  @spec find_schema_hash([map()], map(), Endpoint.t()) :: String.t() | nil
  defp find_schema_hash(specs, action, endpoint) do
    specs
    |> Enum.find(fn spec ->
      Map.get(spec, :name) == Map.get(action, :name) and
        Map.get(spec, :via) == {:beam, endpoint.id}
    end)
    |> case do
      nil -> nil
      spec -> Map.get(spec, :schema_hash)
    end
  end

  @spec spec(Endpoint.t(), atom()) :: map()
  defp spec(endpoint, operation) do
    attrs = %{
      id: "beam.#{endpoint.id}.#{operation}",
      name: operation,
      via: {:beam, endpoint.id},
      description: "Deliver #{operation} through Beam endpoint #{endpoint.id}",
      mode: :write,
      visibility: visibility(endpoint, operation),
      schema: schema(operation),
      metadata: %{endpoint: endpoint.id, channel_type: endpoint.type}
    }

    if Code.ensure_loaded?(@spec_module) and function_exported?(@spec_module, :new, 1) do
      apply(@spec_module, :new, [attrs])
    else
      Map.put(attrs, :schema_hash, schema_hash(attrs))
    end
  end

  @spec schema_hash(map()) :: String.t()
  defp schema_hash(attrs) do
    :sha256
    |> :crypto.hash(
      :erlang.term_to_binary(
        {attrs.via, attrs.name, attrs.mode, attrs.schema},
        [:deterministic]
      )
    )
    |> Base.encode16(case: :lower)
  end

  @spec visibility(Endpoint.t(), atom()) :: :deterministic | :both
  defp visibility(%Endpoint{planner_exposure: :all}, _operation), do: :both

  defp visibility(%Endpoint{planner_exposure: exposure}, operation) when is_list(exposure) do
    if operation in exposure, do: :both, else: :deterministic
  end

  defp visibility(%Endpoint{}, _operation), do: :deterministic

  @spec capability_spec(atom(), Endpoint.t()) :: [map()]
  defp capability_spec(capability, endpoint) do
    case Map.fetch(@capability_operations, capability) do
      {:ok, operation} -> [spec(endpoint, operation)]
      :error -> []
    end
  end

  @spec schema(atom()) :: map()
  defp schema(:send_text) do
    %{
      type: :object,
      required: [:to, :text],
      properties: %{to: %{type: :string}, text: %{type: :string}}
    }
  end

  defp schema(operation) do
    field = Map.fetch!(@operation_content, operation)

    %{
      type: :object,
      required: [:to, field],
      properties: %{field => %{type: :object}, to: %{type: :string}}
    }
  end

  @spec validate_action(map(), Endpoint.t()) :: :ok | {:error, term()}
  defp validate_action(%{via: {:beam, id}, name: operation}, %Endpoint{id: id}) do
    if operation in Map.values(@capability_operations),
      do: :ok,
      else: {:error, {:unsupported_beam_operation, id, operation}}
  end

  defp validate_action(action, endpoint),
    do: {:error, {:beam_action_provider_mismatch, Map.get(action, :via), endpoint.id}}

  @spec content(atom(), map(), map()) :: {:ok, Content.t()} | {:error, term()}
  defp content(:send_text, args, ctx) do
    with {:ok, text} <- resolve_value(arg(args, :text), ctx),
         true <- is_binary(text) and String.trim(text) != "" do
      {:ok, Content.text(text)}
    else
      false -> {:error, :invalid_beam_text}
      {:error, _reason} = error -> error
    end
  end

  defp content(operation, args, ctx) do
    type = Map.fetch!(@operation_content, operation)

    with {:ok, data} <- resolve_value(arg(args, type), ctx),
         false <- is_nil(data) do
      {:ok, Content.new(%{type: type, data: data})}
    else
      true -> {:error, {:missing_beam_content, type}}
      {:error, _reason} = error -> error
    end
  end

  @spec resolve_value(term(), map()) :: {:ok, term()} | {:error, term()}
  defp resolve_value(function, ctx) when is_atom(function) and not is_nil(function) do
    agent = Map.get(ctx, :agent)

    cond do
      is_atom(agent) and function_exported?(agent, function, 2) ->
        normalize_value(apply(agent, function, [Map.get(ctx, :input), ctx]))

      is_atom(agent) and function_exported?(agent, function, 1) ->
        normalize_value(apply(agent, function, [ctx]))

      true ->
        {:ok, function}
    end
  rescue
    exception -> {:error, {:beam_value_resolver_exception, function, exception.__struct__}}
  catch
    kind, reason -> {:error, {:beam_value_resolver_failure, function, kind, reason}}
  end

  defp resolve_value(value, _ctx), do: {:ok, value}

  @spec normalize_value(term()) :: {:ok, term()} | {:error, term()}
  defp normalize_value({:ok, value}), do: {:ok, value}
  defp normalize_value({:error, _reason} = error), do: error
  defp normalize_value(value), do: {:ok, value}

  @spec resolve_target(term(), Endpoint.t(), map()) :: {:ok, term()} | {:error, term()}
  defp resolve_target(nil, endpoint, _ctx), do: {:error, {:missing_beam_target, endpoint.id}}
  defp resolve_target(target, %Endpoint{target_resolver: nil}, _ctx), do: {:ok, target}

  defp resolve_target(target, endpoint, ctx) do
    resolver = endpoint.target_resolver

    cond do
      is_atom(resolver) and function_exported?(resolver, :resolve, 3) ->
        resolver.resolve(target, endpoint, ctx)

      is_atom(resolver) and function_exported?(resolver, :resolve, 2) ->
        resolver.resolve(target, endpoint)

      is_function(resolver, 3) ->
        resolver.(target, endpoint, ctx)

      true ->
        {:error, {:invalid_beam_target_resolver, endpoint.id, resolver}}
    end
  rescue
    exception -> {:error, {:beam_target_resolver_exception, endpoint.id, exception.__struct__}}
  catch
    kind, reason -> {:error, {:beam_target_resolver_failure, endpoint.id, kind, reason}}
  end

  @spec arg(map(), atom(), term()) :: term()
  defp arg(args, key, default \\ nil) when is_map(args) and is_atom(key) do
    case Map.fetch(args, key) do
      {:ok, value} -> value
      :error -> Map.get(args, Atom.to_string(key), default)
    end
  end
end
