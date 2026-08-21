defmodule Mix.Tasks.Beam.Doctor do
  @shortdoc "Checks that a Beam gateway can actually deliver"

  @moduledoc """
  Verifies the runtime processes, the configured agent and store, and every
  endpoint's adapter, server and outbox.

      mix beam.doctor
      mix beam.doctor --socket /run/beam/gateway.sock

  Exits with status 1 when any check fails, so it works in a deploy pipeline.
  """

  use Mix.Task

  alias Spectre.Beam.CLI

  @impl Mix.Task
  def run(args) do
    {opts, _rest} = CLI.parse(args)
    unless opts[:socket] || opts[:port], do: Mix.Task.run("app.start")

    case CLI.connect(opts) do
      {:ok, mode} ->
        try do
          CLI.doctor(mode)
        after
          CLI.disconnect(mode)
        end

      {:error, reason} ->
        Mix.raise("cannot reach a Beam gateway: #{inspect(reason)}")
    end
  end
end
