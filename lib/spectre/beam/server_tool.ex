defmodule Spectre.Beam.ServerTool do
  @moduledoc """
  Stack tool adapter for AgentServer. Inspection is cognitive; delivery is
  exposed only when the Stack binds it to a governed host operation.

  `execute/2` is a host-executor primitive, called after GAM admission. It is
  never used by the cognitive dispatcher. The host owns authorization and
  reconciliation; Beam owns provider delivery and idempotency receipts.
  """
  alias Spectre.Beam.{Config, Endpoint, Outbound, Receipt, Runtime}
  @value :"Elixir.Spectre.GAM.Canonical.Value"

  if Code.ensure_loaded?(Spectre.Server.GAM.EffectAdapter) do
    @behaviour Spectre.Server.GAM.EffectAdapter
  end

  @doc "Declares the execution adapter for explicitly bound delivery."
  @spec effects(map()) :: map()
  def effects(config) do
    Map.new(bindings!(config), fn {:deliver, operation} ->
      {operation,
       %{
         contract: 1,
         adapter: __MODULE__,
         operation: :deliver,
         side_effect: :non_idempotent,
         material: :arguments,
         partition: [],
         schema_hash: nil,
         config: %{}
       }}
    end)
  end

  @doc "Delivers an admitted effect and distinguishes known no-effect from ambiguity."
  @spec execute_effect(:deliver, map(), map()) ::
          {:ok, term()} | {:error, :definitive_no_effect | :ambiguous, term()}
  def execute_effect(:deliver, %{endpoint: endpoint, outbound: attrs} = args, context) do
    case validate_effect(endpoint, attrs, context) do
      :ok ->
        case execute(args, context) do
          {:ok, receipt} -> {:ok, receipt}
          {:error, {:ambiguous, reason}} -> {:error, :ambiguous, reason}
          {:error, reason} -> {:error, :ambiguous, reason}
        end

      {:error, reason} ->
        {:error, :definitive_no_effect, portable_reason(reason)}
    end
  end

  def execute_effect(_, _, _), do: {:error, :definitive_no_effect, :invalid_delivery_request}

  @spec validate_effect(term(), map(), map()) :: :ok | {:error, term()}
  defp validate_effect(endpoint, attrs, context) do
    with {:ok, config} <- Spectre.Beam.config(context.agent),
         {:ok, _} <- Config.fetch(config, endpoint),
         %Outbound{} <-
           Outbound.new(
             Map.merge(attrs, %{endpoint: endpoint, idempotency_key: context.command_id})
           ) do
      :ok
    end
  rescue
    ArgumentError -> {:error, :invalid_delivery_request}
  end

  def tools(config) do
    bindings = bindings!(config)
    operations = %{endpoints: %{path: :cognitive, retry: :safe, max_bytes: 1_048_576}}

    operations =
      case Keyword.get(bindings, :deliver) do
        nil ->
          operations

        operation ->
          Map.put(operations, :deliver, %{
            path: :governed,
            operation: operation,
            max_bytes: 1_048_576
          })
      end

    %{beam: %{contract: 1, adapter: __MODULE__, default: nil, operations: operations}}
  end

  def prepare(:endpoints, [], _trusted), do: {:ok, %{}}

  def prepare(:deliver, [endpoint, attrs], trusted) when is_map(attrs) do
    config = Spectre.Beam.new(trusted.config.channels)

    with {:ok, _endpoint} <- Config.fetch(config, endpoint),
         true <- Map.keys(attrs) -- [:to, :content, :conversation_id, :reply_to] == [],
         %Outbound{} <-
           Outbound.new(Map.merge(attrs, %{endpoint: endpoint, idempotency_key: "validation"})) do
      {:ok, %{endpoint: endpoint, outbound: attrs}}
    else
      false -> {:error, :unsupported_delivery_fields}
      {:error, _} = error -> error
    end
  rescue
    ArgumentError -> {:error, :invalid_delivery_request}
  end

  def prepare(_operation, _payload, _trusted), do: {:error, :invalid_beam_tool_request}

  def invoke(:endpoints, _args, context) do
    config = Spectre.Beam.new(context.installation.config.channels)

    Enum.reduce_while(config.endpoints, {:ok, []}, fn endpoint, {:ok, entries} ->
      case Endpoint.capabilities(endpoint) do
        {:ok, capabilities} ->
          {:cont,
           {:ok,
            [
              %{id: endpoint.id, type: endpoint.type, capabilities: Enum.sort(capabilities)}
              | entries
            ]}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  def project(:endpoints, {:ok, entries}), do: {:ok, {:ok, Enum.reverse(entries)}}
  def project(_operation, {:error, reason}), do: {:ok, {:error, portable_reason(reason)}}
  def project(_operation, _result), do: {:error, :invalid_beam_tool_result}

  @doc "Delivers once through Beam using the admitted command's stable identity."
  def execute(%{endpoint: endpoint, outbound: attrs}, context) do
    with {:ok, config} <- Spectre.Beam.config(context.agent),
         id when is_binary(id) <- Map.get(context, :command_id),
         outbound <- Outbound.new(Map.merge(attrs, %{endpoint: endpoint, idempotency_key: id})),
         {:ok, %Receipt{} = receipt} <-
           Runtime.deliver(config, endpoint, outbound, retry: [max_attempts: 1]) do
      {:ok,
       %{
         endpoint: receipt.endpoint,
         outbound_id: receipt.outbound_id,
         provider_message_id: receipt.provider_message_id,
         status: receipt.status,
         occurred_at: DateTime.to_iso8601(receipt.occurred_at)
       }}
    else
      nil -> {:error, :command_id_required}
      {:error, reason} -> {:error, portable_reason(reason)}
    end
  end

  defp portable_reason({:beam_delivery_in_progress, id}),
    do: {:ambiguous, {:beam_delivery_in_progress, id}}

  defp portable_reason({:ambiguous, reason}), do: {:ambiguous, portable_reason(reason)}

  defp portable_reason(reason) do
    # Spectre is late-bound so standalone Beam still has no runtime core dependency.
    # credo:disable-for-next-line Credo.Check.Refactor.Apply
    case apply(@value, :validate, [reason, [max_bytes: 4096]]) do
      :ok -> reason
      {:error, _} -> :beam_delivery_error_details_unavailable
    end
  end

  defp bindings!(config) do
    bindings = Keyword.get(Map.get(config, :options, []), :bind, [])

    unless Keyword.keyword?(bindings) and
             length(Keyword.keys(bindings)) == length(Enum.uniq(Keyword.keys(bindings))) and
             Enum.all?(bindings, fn {key, operation} ->
               key == :deliver and is_atom(operation) and operation not in [nil, true, false]
             end) do
      raise ArgumentError, "beam bind: expects unique deliver: host_operation entries"
    end

    bindings
  end
end
