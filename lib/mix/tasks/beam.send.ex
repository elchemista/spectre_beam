defmodule Mix.Tasks.Beam.Send do
  @shortdoc "Sends one message to a Beam conversation"

  @moduledoc """
  Sends a message and prints the agent's reply.

      mix beam.send console:support "quanti ticket aperti?"
      mix beam.send telegram:12345 "il report è pronto" --push

  `--push` delivers the text to the conversation without producing a turn,
  which is what a notification is.

  ## Options

    * `--push` — deliver without asking the agent.
    * `--gateway`, `--socket`, `--port`, `--timeout`, `--sender`.
  """

  use Mix.Task

  alias Spectre.Beam.CLI

  @impl Mix.Task
  def run(args) do
    {push?, args} = pop_push(args)
    {opts, rest} = CLI.parse(args)
    unless opts[:socket] || opts[:port], do: Mix.Task.run("app.start")

    case rest do
      [target, text | _extra] -> dispatch(opts, target, text, push?)
      _missing -> Mix.raise("usage: mix beam.send REF TEXT")
    end
  end

  defp dispatch(opts, target, text, push?) do
    case CLI.connect(opts) do
      {:ok, mode} ->
        try do
          if push?,
            do: CLI.push(mode, target, text, opts),
            else: CLI.ask(mode, target, text, opts)
        after
          CLI.disconnect(mode)
        end

      {:error, reason} ->
        Mix.raise("cannot reach a Beam gateway: #{inspect(reason)}")
    end
  end

  defp pop_push(args) do
    case Enum.split_with(args, &(&1 == "--push")) do
      {[], rest} -> {false, rest}
      {_pushes, rest} -> {true, rest}
    end
  end
end
