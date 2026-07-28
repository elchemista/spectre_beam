defmodule Spectre.Beam.ActionProvider do
  @moduledoc """
  Universal provider for proactive Beam actions.
  """

  @behaviour Spectre.Action.Provider

  alias Spectre.Action
  alias Spectre.Action.Spec
  alias Spectre.Beam.Content
  alias Spectre.Beam.Endpoint
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Runtime

  @capability_operations %{
    text: :send_text,
    document: :send_document,
    location: :send_location,
    contact: :send_contact,
    event: :send_event
  }

  @impl true
  def actions(opts) do
    endpoint = Keyword.fetch!(opts, :endpoint)

    with {:ok, capabilities} <- Endpoint.capabilities(endpoint) do
      capabilities
      |> Enum.flat_map(fn capability ->
        case Map.fetch(@capability_operations, capability) do
          {:ok, operation} -> [spec(endpoint, operation)]
          :error -> []
        end
      end)
      |> Enum.sort_by(& &1.name)
    end
  end

  @impl true
  def execute(%Action{} = action, ctx, opts) do
    endpoint = Keyword.fetch!(opts, :endpoint)

    with :ok <- validate_action(action, endpoint),
         {:ok, target} <- resolve_target(arg(action.args, :to), endpoint, ctx),
         {:ok, content} <- content(action.name, action.args, ctx),
         idempotency_key when is_binary(idempotency_key) <-
           Keyword.get(ctx.opts, :idempotency_key),
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
      Runtime.deliver(endpoint, outbound, Keyword.put(ctx.opts, :agent, ctx.agent))
    else
      nil -> {:error, :missing_beam_idempotency_key}
      {:error, _reason} = error -> error
    end
  end

  @impl true
  def schema_hash(%Action{} = action, opts) do
    endpoint = Keyword.fetch!(opts, :endpoint)

    case actions(opts) do
      specs when is_list(specs) ->
        case Enum.find(specs, &(&1.name == action.name and &1.via == {:beam, endpoint.id})) do
          %Spec{schema_hash: hash} -> hash
          nil -> nil
        end

      {:error, _reason} ->
        nil
    end
  end

  @spec spec(Endpoint.t(), atom()) :: Spec.t()
  defp spec(endpoint, operation) do
    Spec.new(%{
      id: "beam.#{endpoint.id}.#{operation}",
      name: operation,
      via: {:beam, endpoint.id},
      description: "Deliver #{operation} through Beam endpoint #{endpoint.id}",
      mode: :write,
      visibility: visibility(endpoint, operation),
      schema: schema(operation),
      metadata: %{endpoint: endpoint.id, channel_type: endpoint.type}
    })
  end

  @spec visibility(Endpoint.t(), atom()) :: :deterministic | :both
  defp visibility(%Endpoint{planner_exposure: :all}, _operation), do: :both

  defp visibility(%Endpoint{planner_exposure: exposure}, operation) when is_list(exposure) do
    if operation in exposure, do: :both, else: :deterministic
  end

  defp visibility(%Endpoint{}, _operation), do: :deterministic

  @spec schema(atom()) :: map()
  defp schema(:send_text) do
    %{
      type: :object,
      required: [:to, :text],
      properties: %{to: %{type: :string}, text: %{type: :string}}
    }
  end

  defp schema(operation) do
    field =
      operation |> Atom.to_string() |> String.replace_prefix("send_", "") |> String.to_atom()

    %{
      type: :object,
      required: [:to, field],
      properties: %{field => %{type: :object}, to: %{type: :string}}
    }
  end

  @spec validate_action(Action.t(), Endpoint.t()) :: :ok | {:error, term()}
  defp validate_action(%Action{via: {:beam, id}, name: operation}, %Endpoint{id: id}) do
    if operation in Map.values(@capability_operations),
      do: :ok,
      else: {:error, {:unsupported_beam_operation, id, operation}}
  end

  defp validate_action(%Action{} = action, endpoint),
    do: {:error, {:beam_action_provider_mismatch, action.via, endpoint.id}}

  @spec content(atom(), map(), Spectre.Context.t()) :: {:ok, Content.t()} | {:error, term()}
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
    type = operation |> Atom.to_string() |> String.replace_prefix("send_", "") |> String.to_atom()

    with {:ok, data} <- resolve_value(arg(args, type), ctx),
         false <- is_nil(data) do
      {:ok, Content.new(%{type: type, data: data})}
    else
      true -> {:error, {:missing_beam_content, type}}
      {:error, _reason} = error -> error
    end
  end

  @spec resolve_value(term(), Spectre.Context.t()) :: {:ok, term()} | {:error, term()}
  defp resolve_value(function, ctx) when is_atom(function) and not is_nil(function) do
    cond do
      function_exported?(ctx.agent, function, 2) ->
        normalize_value(apply(ctx.agent, function, [ctx.input, ctx]))

      function_exported?(ctx.agent, function, 1) ->
        normalize_value(apply(ctx.agent, function, [ctx]))

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

  @spec resolve_target(term(), Endpoint.t(), Spectre.Context.t()) ::
          {:ok, term()} | {:error, term()}
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
