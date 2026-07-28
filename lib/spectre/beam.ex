defmodule Spectre.Beam do
  @moduledoc """
  Multichannel input and output boundary for Spectre Agents.

  Beam decodes provider-specific events into `Spectre.Input`, delegates the
  turn to the canonical `Spectre.turn/3` boundary, and delivers an ordinary
  reactive reply through the same endpoint. Proactive messages remain normal
  Spectre actions subject to policy and lifecycle.
  """

  alias Spectre.Stack.DSL

  @version "0.1.2"

  use Spectre.Stack.Installable,
    id: :beam,
    version: @version,
    contract: 1,
    spectre: "~> 0.1.2",
    provides: [{:service, :beam}],
    agent_extensions: [Spectre.Beam.Extension],
    dsl: __MODULE__

  @doc """
  Returns the Beam package version.
  """
  @spec version() :: String.t()
  def version, do: @version

  defmacro __using__(opts) do
    quote do
      import Spectre.Beam, only: [beaming: 1, channel: 2, beam: 2]

      Spectre.Extension.register!(
        __MODULE__,
        Spectre.Beam.Extension,
        unquote(opts)
      )
    end
  end

  @doc """
  Groups endpoint declarations on an Agent.
  """
  defmacro beaming(do: block), do: block

  @doc """
  Declares one mounted external endpoint.
  """
  defmacro channel(id, opts) do
    id = expand_value(id, __CALLER__)
    opts = expand_value(opts, __CALLER__)
    declaration = {id, opts}

    quote do
      @spectre_beam_channels unquote(Macro.escape(declaration))
    end
  end

  @doc """
  Stages a proactive Beam action.

  `via:` selects the mounted endpoint. The operation defaults to `:send_text`
  and can be overridden explicitly with `operation:`.
  """
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
      [
        args: args,
        mode: :write
      ]
      |> maybe_put(:policy, Keyword.get(opts, :policy))
      |> maybe_put(:reply, Keyword.get(opts, :reply))

    quote do
      action(
        unquote(Macro.escape({:beam, endpoint, operation})),
        unquote(Macro.escape(action_opts))
      )
    end
  end

  @doc """
  Returns an Agent's compiled Beam configuration.
  """
  @spec config(module()) :: {:ok, Spectre.Beam.Config.t()} | {:error, term()}
  def config(agent) when is_atom(agent) do
    with {:ok, mount} <- Spectre.Extension.fetch(agent, :beam),
         %Spectre.Beam.Config{} = config <- mount.compiled do
      {:ok, config}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :invalid_beam_configuration}
    end
  end

  @doc """
  Decodes one provider event through a mounted endpoint.
  """
  defdelegate decode(agent, endpoint, event, opts \\ []), to: Spectre.Beam.Runtime

  @doc """
  Converts a normalized Beam inbound into a Spectre input.
  """
  defdelegate to_input(inbound), to: Spectre.Beam.Runtime

  @doc """
  Delivers the visible reply from a result through the inbound endpoint.
  """
  defdelegate reply(agent, inbound, result, opts \\ []), to: Spectre.Beam.Runtime

  @doc """
  Runs decode, deduplication, `Spectre.turn/3`, and reactive delivery.
  """
  defdelegate handle(agent_or_session, endpoint, event, opts \\ []),
    to: Spectre.Beam.Runtime

  @doc """
  Subscribes the calling process through a configured endpoint adapter.
  """
  defdelegate subscribe(agent, endpoint, opts \\ []), to: Spectre.Beam.Runtime

  @doc """
  Unsubscribes the calling process through a configured endpoint adapter.
  """
  defdelegate unsubscribe(agent, endpoint, opts \\ []), to: Spectre.Beam.Runtime

  @impl Spectre.Stack.Installable
  def compile(opts, block, caller) do
    channels =
      block
      |> DSL.compile!(caller, channel: 2)
      |> Enum.map(fn {:channel, [id, adapter]} -> {id, adapter} end)

    case duplicate_id(channels) do
      :none -> {:ok, %{options: opts, channels: channels}}
      {:duplicate, id} -> {:error, {:duplicate_beam_channel, id}}
    end
  end

  @spec duplicate_id([{term(), term()}]) :: :none | {:duplicate, term()}
  defp duplicate_id(entries) do
    entries
    |> Enum.reduce_while(MapSet.new(), fn {id, _adapter}, seen ->
      if MapSet.member?(seen, id),
        do: {:halt, id},
        else: {:cont, MapSet.put(seen, id)}
    end)
    |> case do
      %MapSet{} -> :none
      id -> {:duplicate, id}
    end
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
