defmodule Spectre.Beam.Chat do
  @moduledoc """
  The OTP surface for talking to an agent from inside the node.

  A LiveView, an IEx console, a CLI over the local socket and a test all use
  this and nothing else. It is deliberately small and deliberately
  non-blocking: `send/3` returns as soon as the message is queued, and answers
  arrive as `Spectre.Beam.Event` messages in the caller's mailbox. A turn is
  never awaited inside a `handle_event/3`.

  ## LiveView

      def mount(%{"id" => id}, _session, socket) do
        {:ok, ref} = Chat.open(MyApp.Gateway, "web:" <> id)
        if connected?(socket), do: Chat.subscribe(ref)

        {:ok,
         socket
         |> assign(ref: ref)
         |> stream(:messages, Chat.history(ref, limit: 50))}
      end

      def handle_event("send", %{"text" => text}, socket) do
        {:ok, _ref} = Chat.send(socket.assigns.ref, text)
        {:noreply, socket}
      end

      def handle_info(%Event{type: :reply, payload: %{text: text}}, socket), do: ...
      def handle_info(%Event{type: :typing, payload: %{composing?: t}}, socket), do: ...

  Every event carries a monotonic `seq`. After a reconnect, ask for what was
  missed with `history(ref, after: last_seq)` instead of replaying everything.

  ## Sending versus pushing

  `send/3` injects a message *as the conversation's user*: it runs the inbound
  pipeline and produces a turn. `push/3` delivers *to* the conversation
  without a turn, which is what a notification or an operator message is.
  """

  alias Spectre.Beam.Bus
  alias Spectre.Beam.Content
  alias Spectre.Beam.Conversation
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Gateway.Spec
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Ref
  alias Spectre.Beam.Runtime

  @default_timeout :timer.seconds(60)
  @default_sender "local"

  @doc """
  Ensures the conversation exists and returns its canonical reference.
  """
  @spec open(atom(), Gateway.target(), keyword()) :: {:ok, Ref.t()} | {:error, term()}
  defdelegate open(gateway, target, opts \\ []), to: Gateway

  @doc """
  Queues a message from the conversation's user and returns immediately.

  The reply arrives as a `:reply` event on the bus, and reaches the provider
  through the endpoint outbox.
  """
  @spec send(Ref.t(), String.t() | Content.t(), keyword()) ::
          {:ok, Ref.t()} | {:duplicate, Ref.t()} | {:error, term()}
  def send(%Ref{} = ref, text, opts \\ []) do
    with {:ok, spec} <- Gateway.spec(ref.gateway),
         {:ok, inbound} <- build_inbound(spec, ref, text, opts),
         {:ok, inbound} <- Runtime.finish_decode(spec.beam, ref.endpoint, inbound, opts) do
      Gateway.ingest_inbound(ref.gateway, inbound, opts)
    end
  end

  @doc """
  Sends a message and waits for the agent's reply text.

  Convenient from IEx, a script, or a test. A UI should use `send/3` and read
  events instead, so a slow model never blocks the process handling it.

  Options: `:timeout` (default one minute) plus everything `send/3` accepts.
  """
  @spec ask(Ref.t(), String.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def ask(%Ref{} = ref, text, opts \\ []) do
    with {:ok, timeout} <- timeout(opts) do
      do_ask(ref, text, opts, timeout)
    end
  end

  @spec do_ask(Ref.t(), String.t(), keyword(), timeout()) ::
          {:ok, String.t()} | {:error, term()}
  defp do_ask(ref, text, opts, timeout) do
    owner = self()

    waiter =
      Task.async(fn ->
        :ok = subscribe(ref)
        Process.send(owner, {:beam_chat_ready, self()}, [])
        await_reply(ref, deadline(timeout))
      end)

    receive do
      {:beam_chat_ready, _pid} -> :ok
    after
      5_000 -> :ok
    end

    case send(ref, text, opts) do
      {:ok, _ref} ->
        Task.await(waiter, task_timeout(timeout))

      {:duplicate, _ref} ->
        _stopped = Task.shutdown(waiter, :brutal_kill)
        {:error, :duplicate_beam_inbound}

      {:error, _reason} = error ->
        _stopped = Task.shutdown(waiter, :brutal_kill)
        error
    end
  end

  @doc """
  Delivers a message to the conversation without producing a turn.
  """
  @spec push(Ref.t(), String.t() | Content.t(), keyword()) :: {:ok, Ref.t()} | {:error, term()}
  def push(%Ref{} = ref, content, opts \\ []), do: Gateway.push(ref.gateway, ref, content, opts)

  @doc "Subscribes the calling process to a conversation's events."
  @spec subscribe(Ref.t()) :: :ok | {:error, term()}
  def subscribe(%Ref{} = ref) do
    with {:ok, spec} <- Gateway.spec(ref.gateway),
         do: Bus.subscribe(spec.bus, Ref.topic(ref))
  end

  @doc "Subscribes the calling process to every conversation on one endpoint."
  @spec subscribe_endpoint(atom(), term()) :: :ok | {:error, term()}
  def subscribe_endpoint(gateway, endpoint_id) do
    with {:ok, spec} <- Gateway.spec(gateway),
         do: Bus.subscribe(spec.bus, {:endpoint, gateway, endpoint_id})
  end

  @doc "Removes the calling process' subscription."
  @spec unsubscribe(Ref.t()) :: :ok
  def unsubscribe(%Ref{} = ref) do
    case Gateway.spec(ref.gateway) do
      {:ok, spec} -> Bus.unsubscribe(spec.bus, Ref.topic(ref))
      {:error, _reason} -> :ok
    end
  end

  @doc """
  Returns retained events, oldest first.

  Options: `:after` (only events past this sequence), `:limit`, `:types`.
  """
  @spec history(Ref.t(), keyword()) :: [Event.t()]
  defdelegate history(ref, opts \\ []), to: Conversation

  @doc "Stops the turn in flight, keeping anything already queued."
  @spec cancel(Ref.t()) :: :ok
  defdelegate cancel(ref), to: Conversation

  @doc "Returns the conversation's observable state."
  @spec status(Ref.t()) :: {:ok, map()} | {:error, :not_found}
  defdelegate status(ref), to: Conversation

  @doc "Stops the conversation process and drops its retained transcript."
  @spec close(Ref.t()) :: :ok
  def close(%Ref{} = ref), do: Gateway.close(ref.gateway, ref)

  @spec build_inbound(Spec.t(), Ref.t(), String.t() | Content.t(), keyword()) ::
          {:ok, Inbound.t()} | {:error, term()}
  defp build_inbound(spec, ref, text, opts) do
    with {:ok, endpoint} <- Spec.endpoint(spec, ref.endpoint) do
      {:ok,
       Inbound.new(%{
         endpoint: ref.endpoint,
         channel_type: endpoint.type,
         message_id: Keyword.get(opts, :message_id) || generated_id(),
         conversation_id: ref.conversation_id,
         sender: Keyword.get(opts, :sender, @default_sender),
         recipient: Keyword.get(opts, :recipient),
         content: content(text),
         authenticated?: Keyword.get(opts, :authenticated?, false),
         occurred_at: DateTime.utc_now(),
         metadata: Keyword.get(opts, :metadata, %{origin: :local})
       })}
    end
  rescue
    exception in ArgumentError -> {:error, {:invalid_beam_inbound, Exception.message(exception)}}
  end

  @spec content(String.t() | Content.t()) :: Content.t()
  defp content(%Content{} = content), do: content
  defp content(text) when is_binary(text), do: Content.text(text)

  @spec await_reply(Ref.t(), integer() | :infinity) ::
          {:ok, String.t()} | {:error, term()}
  defp await_reply(ref, deadline) do
    slug = Ref.slug(ref)

    receive do
      %Event{type: :reply, ref: %Ref{} = event_ref, payload: %{text: text}} ->
        if Ref.slug(event_ref) == slug, do: {:ok, text}, else: await_reply(ref, deadline)

      %Event{type: :error, ref: %Ref{} = event_ref, payload: %{reason: reason}} ->
        if Ref.slug(event_ref) == slug, do: {:error, reason}, else: await_reply(ref, deadline)

      %Event{type: :status, payload: %{status: :transport_only}} ->
        {:error, :beam_transport_only}

      %Event{} ->
        await_reply(ref, deadline)
    after
      remaining(deadline) -> {:error, :timeout}
    end
  end

  @spec timeout(keyword()) :: {:ok, timeout()} | {:error, term()}
  defp timeout(opts) do
    case Keyword.get(opts, :timeout, @default_timeout) do
      :infinity -> {:ok, :infinity}
      timeout when is_integer(timeout) and timeout >= 0 -> {:ok, timeout}
      timeout -> {:error, {:invalid_beam_timeout, timeout}}
    end
  end

  @spec deadline(timeout()) :: integer() | :infinity
  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  @spec remaining(integer() | :infinity) :: timeout()
  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  @spec task_timeout(timeout()) :: timeout()
  defp task_timeout(:infinity), do: :infinity
  defp task_timeout(timeout), do: timeout + 5_000

  @spec generated_id() :: String.t()
  defp generated_id,
    do: "chat-" <> Base.url_encode64(:crypto.strong_rand_bytes(9), padding: false)
end
