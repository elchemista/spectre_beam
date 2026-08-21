defmodule Mix.Tasks.Beam.Tail do
  @shortdoc "Follows a Beam conversation live"

  @moduledoc """
  Prints a conversation's retained transcript and then follows it until
  interrupted.

      mix beam.tail telegram:12345
      mix beam.tail console:support --limit 50
      mix beam.tail telegram:12345 --socket /run/beam/gateway.sock
  """

  use Mix.Task

  alias Spectre.Beam.CLI

  @impl Mix.Task
  def run(args) do
    {opts, rest} = CLI.parse(args)
    unless opts[:socket] || opts[:port], do: Mix.Task.run("app.start")

    case rest do
      [target | _extra] -> follow(opts, target)
      [] -> Mix.raise("usage: mix beam.tail REF")
    end
  end

  defp follow(opts, target) do
    case CLI.connect(opts) do
      {:ok, mode} ->
        try do
          CLI.tail(mode, target, opts)
        after
          CLI.disconnect(mode)
        end

      {:error, reason} ->
        Mix.raise("cannot reach a Beam gateway: #{inspect(reason)}")
    end
  end
end
