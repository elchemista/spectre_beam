defmodule Spectre.Beam.Console do
  @moduledoc """
  Interactive terminal conversation with an agent.

  `chat/2` takes over the shell: it opens a conversation on a local channel,
  subscribes to it, prints events as they arrive, and reads the next line.
  Because it goes through a real `Spectre.Beam.Channel`, everything a
  production channel passes through — pipelines, idempotency, throttling,
  policy — applies to the console too.

      iex> Spectre.Beam.Console.chat()

  Slash commands are handled by the console and never reach the agent:

      /help              this list
      /new               start a fresh conversation
      /who               gateway, endpoint, agent and scope
      /history [n]       replay the retained transcript
      /stop              cancel the turn in flight
      /endpoint <id>     switch channel
      /exit              leave

  With `Spectre.Beam.IEx` imported the one-line helpers are usually enough;
  `chat/2` is for an actual back-and-forth.
  """

  alias Spectre.Beam.Chat
  alias Spectre.Beam.Conversation
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Gateway.Spec
  alias Spectre.Beam.Ref

  @local_adapters [Spectre.Beam.Adapters.Local, Spectre.Beam.Adapters.Test]
  @turn_timeout :timer.minutes(5)
  @prompt "you › "

  @doc """
  Starts an interactive conversation and blocks until `/exit`.

  Options: `:gateway`, `:endpoint`, `:conversation`, `:timeout`.
  """
  @spec chat(Gateway.target() | nil, keyword()) :: :ok
  def chat(target \\ nil, opts \\ []) do
    case open(target, opts) do
      {:ok, ref} ->
        :ok = Chat.subscribe(ref)
        banner(ref)
        loop(ref, opts)
        Chat.unsubscribe(ref)
        :ok

      {:error, reason} ->
        IO.puts(:stderr, "cannot open a conversation: #{inspect(reason)}")
        :ok
    end
  end

  @doc """
  Follows a conversation without sending anything, until interrupted.
  """
  @spec tail(Ref.t(), keyword()) :: :ok
  def tail(%Ref{} = ref, opts \\ []) do
    :ok = Chat.subscribe(ref)

    ref
    |> Chat.history(Keyword.take(opts, [:limit, :after]))
    |> Enum.each(&print/1)

    IO.puts("— following #{Ref.slug(ref)}, interrupt to stop —")
    follow(ref, Keyword.get(opts, :timeout, :infinity))
  end

  @doc """
  Resolves the conversation a console should attach to.

  With no target it picks the only running gateway, prefers a channel served
  by a local adapter, and names a fresh conversation.
  """
  @spec open(Gateway.target() | nil, keyword()) :: {:ok, Ref.t()} | {:error, term()}
  def open(target, opts \\ [])

  def open(%Ref{} = ref, opts), do: Gateway.open(ref.gateway, ref, opts)

  def open(target, opts) when is_binary(target) do
    with {:ok, gateway} <- gateway(opts), do: Gateway.open(gateway, target, opts)
  end

  def open(nil, opts) do
    with {:ok, gateway} <- gateway(opts),
         {:ok, spec} <- Gateway.spec(gateway),
         {:ok, endpoint_id} <- endpoint(spec, opts) do
      conversation = Keyword.get(opts, :conversation) || generated_conversation()
      Gateway.open(gateway, "#{endpoint_id}:#{conversation}", opts)
    end
  end

  @spec gateway(keyword()) :: {:ok, atom()} | {:error, term()}
  defp gateway(opts) do
    case Keyword.get(opts, :gateway) do
      name when is_atom(name) and not is_nil(name) ->
        {:ok, name}

      _unset ->
        case Gateway.list() do
          [only] -> {:ok, only}
          [] -> {:error, :no_beam_gateway_running}
          many -> {:error, {:ambiguous_beam_gateway, many}}
        end
    end
  end

  @spec endpoint(Spec.t(), keyword()) :: {:ok, term()} | {:error, term()}
  defp endpoint(spec, opts) do
    case Keyword.get(opts, :endpoint) do
      nil ->
        endpoints = Spec.endpoints(spec)

        case Enum.find(endpoints, &(&1.adapter in @local_adapters)) || List.first(endpoints) do
          nil -> {:error, :no_beam_endpoint_configured}
          endpoint -> {:ok, endpoint.id}
        end

      id ->
        with {:ok, endpoint} <- Spec.endpoint(spec, id), do: {:ok, endpoint.id}
    end
  end

  @spec loop(Ref.t(), keyword()) :: :ok
  defp loop(ref, opts) do
    drain()

    case IO.gets(@prompt) do
      :eof ->
        :ok

      {:error, reason} ->
        IO.puts(:stderr, "input error: #{inspect(reason)}")
        :ok

      line ->
        case handle(String.trim(line), ref, opts) do
          {:continue, ref} -> loop(ref, opts)
          :halt -> :ok
        end
    end
  end

  @spec handle(String.t(), Ref.t(), keyword()) :: {:continue, Ref.t()} | :halt
  defp handle("", ref, _opts), do: {:continue, ref}
  defp handle("/exit", _ref, _opts), do: :halt
  defp handle("/quit", _ref, _opts), do: :halt

  defp handle("/help", ref, _opts) do
    IO.puts(@moduledoc |> String.split("Slash commands") |> List.last() |> String.trim())
    {:continue, ref}
  end

  defp handle("/who", ref, _opts) do
    case Chat.status(ref) do
      {:ok, status} ->
        IO.puts(
          "  #{Ref.slug(ref)} · agent #{inspect(status.agent)} · scope #{status.scope} · " <>
            "#{status.turns} turns · seq #{status.seq}"
        )

      {:error, :not_found} ->
        IO.puts("  #{Ref.slug(ref)} · not started")
    end

    {:continue, ref}
  end

  defp handle("/new", ref, opts) do
    Chat.unsubscribe(ref)

    case open(nil, Keyword.delete(opts, :conversation)) do
      {:ok, fresh} ->
        :ok = Chat.subscribe(fresh)
        IO.puts("  → #{Ref.slug(fresh)}")
        {:continue, fresh}

      {:error, reason} ->
        IO.puts(:stderr, "  cannot open: #{inspect(reason)}")
        {:continue, ref}
    end
  end

  defp handle("/stop", ref, _opts) do
    :ok = Chat.cancel(ref)
    IO.puts("  cancelled")
    {:continue, ref}
  end

  defp handle("/history" <> rest, ref, _opts) do
    limit =
      case Integer.parse(String.trim(rest)) do
        {value, _remainder} -> value
        :error -> 20
      end

    ref |> Conversation.history(limit: limit) |> Enum.each(&print/1)
    {:continue, ref}
  end

  defp handle("/endpoint " <> id, ref, opts) do
    Chat.unsubscribe(ref)
    opts = Keyword.put(opts, :endpoint, String.to_atom(String.trim(id)))

    case open(nil, Keyword.delete(opts, :conversation)) do
      {:ok, fresh} ->
        :ok = Chat.subscribe(fresh)
        IO.puts("  → #{Ref.slug(fresh)}")
        {:continue, fresh}

      {:error, reason} ->
        IO.puts(:stderr, "  cannot switch: #{inspect(reason)}")
        {:continue, ref}
    end
  end

  defp handle("/" <> unknown, ref, _opts) do
    IO.puts("  unknown command: /#{unknown} — try /help")
    {:continue, ref}
  end

  defp handle(text, ref, opts) do
    case Chat.send(ref, text, Keyword.take(opts, [:sender, :authenticated?])) do
      {:ok, _ref} -> await_turn(ref, Keyword.get(opts, :timeout, @turn_timeout))
      {:duplicate, _ref} -> IO.puts("  (duplicate, ignored)")
      {:error, reason} -> IO.puts(:stderr, "  send failed: #{inspect(reason)}")
    end

    {:continue, ref}
  end

  # The turn is terminal when the conversation reports itself idle again: the
  # reply, or the failure that replaced it, has already been printed by then.
  @spec await_turn(Ref.t(), timeout()) :: :ok
  defp await_turn(ref, timeout) do
    receive do
      %Event{type: :status, payload: %{status: :idle}} ->
        :ok

      %Event{type: :status, payload: %{status: :transport_only}} ->
        IO.puts("  (no agent configured — transport-only gateway)")

      %Event{type: :status} ->
        await_turn(ref, timeout)

      # The line was typed at this prompt a moment ago; echoing it back reads
      # as the agent repeating the user.
      %Event{type: :inbound} ->
        await_turn(ref, timeout)

      %Event{} = event ->
        print(event)
        await_turn(ref, timeout)
    after
      timeout ->
        IO.puts(:stderr, "  no answer after #{timeout}ms")
        :ok
    end
  end

  @spec follow(Ref.t(), timeout()) :: :ok
  defp follow(ref, timeout) do
    receive do
      %Event{} = event ->
        print(event)
        follow(ref, timeout)
    after
      timeout -> :ok
    end
  end

  @spec drain() :: :ok
  defp drain do
    receive do
      %Event{} = event ->
        print(event)
        drain()
    after
      0 -> :ok
    end
  end

  @spec print(Event.t()) :: :ok
  defp print(%Event{type: :reply, payload: %{text: text}}), do: IO.puts("bot › #{text}")

  defp print(%Event{type: :inbound, payload: %{text: text, sender: sender}}) do
    IO.puts("#{sender} › #{text}")
  end

  defp print(%Event{type: :delta, payload: %{text: chunk}}), do: IO.write(chunk)

  defp print(%Event{type: :error, payload: payload}) do
    IO.puts(
      :stderr,
      "  ! #{inspect(Map.get(payload, :stage))}: #{inspect(Map.get(payload, :reason))}"
    )
  end

  defp print(%Event{type: :policy_required, payload: payload}) do
    IO.puts("  ⟡ approval required: #{inspect(payload)}")
  end

  defp print(%Event{}), do: :ok

  @spec banner(Ref.t()) :: :ok
  defp banner(ref) do
    IO.puts("beam · #{Ref.slug(ref)} · #{inspect(ref.agent)} · scope #{ref.scope}")
    IO.puts("/help  /new  /who  /history  /stop  /endpoint ID  /exit\n")
  end

  @spec generated_conversation() :: String.t()
  defp generated_conversation,
    do: "console-" <> Base.url_encode64(:crypto.strong_rand_bytes(4), padding: false)
end
