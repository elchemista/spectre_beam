defmodule Spectre.Beam do
  @moduledoc """
  Stack-installable boundary for external channels.

  Beam owns the package-local `channel/2` declaration. At version 0.1.2 it
  compiles immutable channel configuration only: it does not start adapters,
  publish capabilities, or make a channel visible to an Agent.
  """

  alias Spectre.Stack.DSL

  @version "0.1.2"

  use Spectre.Stack.Installable,
    id: :beam,
    version: @version,
    contract: 1,
    spectre: "~> 0.1.2",
    dsl: __MODULE__

  @doc """
  Returns the Beam package version.
  """
  @spec version() :: String.t()
  def version, do: @version

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
end
