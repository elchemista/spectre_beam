defmodule Mix.Tasks.Beam.Chat do
  @shortdoc "Talks to a Spectre agent from the terminal"

  @moduledoc """
  Opens an interactive conversation with an agent.

      mix beam.chat
      mix beam.chat console:support
      mix beam.chat --endpoint console --gateway MyApp.Gateway
      mix beam.chat --socket /run/beam/gateway.sock

  Without `--socket` the task starts this project's application and talks to a
  gateway in the same VM. With it, the task attaches to a gateway already
  running elsewhere — a release in production, for instance — over its local
  control socket, without loading any of that node's code.

  ## Options

    * `--gateway` — gateway name; required only when several are running.
    * `--endpoint` — channel to open the conversation on.
    * `--socket` / `--port` — attach over the control socket instead.
    * `--timeout` — milliseconds to wait for each answer.
    * `--sender` — sender recorded on the messages.
  """

  use Mix.Task

  alias Spectre.Beam.CLI

  @impl Mix.Task
  def run(args) do
    {opts, rest} = CLI.parse(args)
    unless opts[:socket] || opts[:port], do: Mix.Task.run("app.start")

    case CLI.connect(opts) do
      {:ok, mode} ->
        try do
          CLI.chat(mode, List.first(rest), opts)
        after
          CLI.disconnect(mode)
        end

      {:error, reason} ->
        Mix.raise("cannot reach a Beam gateway: #{inspect(reason)}")
    end
  end
end
