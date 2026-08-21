defmodule Mix.Tasks.Beam.Status do
  @shortdoc "Shows Beam gateway endpoints and conversations"

  @moduledoc """
  Prints the endpoints a gateway serves and the conversations it is holding.

      mix beam.status
      mix beam.status --gateway MyApp.Gateway
      mix beam.status --socket /run/beam/gateway.sock
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
          CLI.status(mode)
        after
          CLI.disconnect(mode)
        end

      {:error, reason} ->
        Mix.raise("cannot reach a Beam gateway: #{inspect(reason)}")
    end
  end
end
