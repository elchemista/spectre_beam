defmodule Spectre.Beam.IEx do
  @moduledoc """
  One-line helpers for driving a gateway from the shell.

  Add the import to `.iex.exs` and the whole gateway is one word away:

      # .iex.exs
      import Spectre.Beam.IEx

  Then:

      iex> endpoints()
      iex> say "quanti ticket aperti abbiamo?"
      iex> say "telegram:12345", "ciao, tutto ok?"
      iex> ask "riassumi l'ultima ora"
      iex> ls()
      iex> tail "telegram:12345"
      iex> doctor()

  `say/1` and its friends address the *current* conversation, remembered in
  the shell process. The first call opens one on a local channel; `focus/1`
  points the helpers somewhere else, and `current/0` shows where they point.
  Because it lives in the shell's own process dictionary, two IEx sessions —
  or a session and a LiveView — never fight over it.

  For an actual back-and-forth rather than one-off calls, use `chat/0`.

  With distribution enabled, `iex --remsh` gives the same helpers against a
  running production node. That is full access to the node, so treat the shell
  as the credential it is.
  """

  alias Spectre.Beam.Chat
  alias Spectre.Beam.Console
  alias Spectre.Beam.Doctor
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Ref

  @current :beam_current_ref

  @doc "Lists the gateways running on this node."
  @spec gateways() :: [atom()]
  defdelegate gateways(), to: Gateway, as: :list

  @doc "Prints one line per mounted endpoint."
  @spec endpoints(atom() | nil) :: :ok
  def endpoints(gateway \\ nil) do
    case resolve_gateway(gateway) do
      {:ok, name} ->
        name |> Gateway.endpoints() |> Enum.each(&IO.puts(endpoint_line(&1)))

      {:error, reason} ->
        IO.puts(:stderr, inspect(reason))
    end
  end

  @spec endpoint_line(map()) :: iodata()
  defp endpoint_line(status) do
    [
      String.pad_trailing(to_string(status.endpoint), 14),
      String.pad_trailing(to_string(Map.get(status, :ingress, :none)), 10),
      String.pad_trailing(format_status(status.status), 12),
      String.pad_trailing("#{Map.get(status, :events, 0)} events", 14),
      format_time(Map.get(status, :last_event_at))
    ]
  end

  @doc "Prints one line per live conversation."
  @spec ls(atom() | nil) :: :ok
  def ls(gateway \\ nil) do
    case resolve_gateway(gateway) do
      {:ok, name} ->
        name |> Gateway.conversations() |> Enum.each(&IO.puts(conversation_line(&1)))

      {:error, reason} ->
        IO.puts(:stderr, inspect(reason))
    end
  end

  @spec conversation_line(Ref.t()) :: iodata()
  defp conversation_line(ref) do
    case Chat.status(ref) do
      {:ok, status} ->
        [
          String.pad_trailing(Ref.slug(ref), 28),
          String.pad_trailing(format_status(status.status), 12),
          String.pad_trailing("#{status.turns} turns", 12),
          "seq #{status.seq}"
        ]

      {:error, :not_found} ->
        Ref.slug(ref)
    end
  end

  @doc "Points the helpers at another conversation."
  @spec focus(module() | Ref.t() | String.t()) :: Ref.t() | {:error, term()}
  def focus(target) do
    case open(target) do
      {:ok, ref} -> put_current(ref)
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns the conversation the helpers currently address."
  @spec current() :: Ref.t() | nil
  def current, do: Process.get(@current)

  @doc """
  Sends a message to the current conversation and prints the reply.
  """
  @spec say(String.t()) :: :ok
  def say(text) when is_binary(text) do
    with {:ok, ref} <- ensure_current(), do: say(ref, text)
  end

  @doc "Sends a message to an explicit conversation and prints the reply."
  @spec say(module() | Ref.t() | String.t(), String.t(), keyword()) :: :ok
  def say(target, text, opts \\ []) when is_binary(text) do
    case ask(target, text, opts) do
      {:ok, reply} -> IO.puts(reply)
      {:error, reason} -> IO.puts(:stderr, "no reply: #{inspect(reason)}")
    end
  end

  @doc "Sends a message and returns the reply text instead of printing it."
  @spec ask(String.t()) :: {:ok, String.t()} | {:error, term()}
  def ask(text) when is_binary(text) do
    with {:ok, ref} <- ensure_current(), do: Chat.ask(ref, text)
  end

  @doc "Sends a message to an explicit conversation and returns the reply text."
  @spec ask(module() | Ref.t() | String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def ask(target, text, opts \\ []) when is_binary(text) do
    with {:ok, ref} <- open(target) do
      _current = put_current(ref)
      Chat.ask(ref, text, opts)
    end
  end

  @doc "Delivers a message to a conversation without producing a turn."
  @spec push(Ref.t() | String.t(), String.t(), keyword()) :: {:ok, Ref.t()} | {:error, term()}
  def push(target, text, opts \\ []) do
    with {:ok, ref} <- open(target), do: Chat.push(ref, text, opts)
  end

  @doc "Starts an interactive conversation in this shell."
  @spec chat(Ref.t() | String.t() | nil, keyword()) :: :ok
  def chat(target \\ nil, opts \\ []) do
    case Console.open(target, opts) do
      {:ok, ref} ->
        _current = put_current(ref)
        Console.chat(ref, opts)

      {:error, reason} ->
        IO.puts(:stderr, inspect(reason))
        :ok
    end
  end

  @doc "Follows a conversation live until interrupted."
  @spec tail(Ref.t() | String.t() | nil, keyword()) :: :ok
  def tail(target \\ nil, opts \\ []) do
    with {:ok, ref} <- resolve(target), do: Console.tail(ref, opts)
  end

  @doc "Prints the retained transcript of a conversation."
  @spec history(Ref.t() | String.t() | nil, keyword()) :: :ok
  def history(target \\ nil, opts \\ []) do
    with {:ok, ref} <- resolve(target) do
      ref
      |> Chat.history(Keyword.put_new(opts, :limit, 20))
      |> Enum.each(&IO.puts(format_event(&1)))
    end
  end

  @doc "Cancels the turn in flight on a conversation."
  @spec cancel(Ref.t() | String.t() | nil) :: :ok
  def cancel(target \\ nil) do
    with {:ok, ref} <- resolve(target), do: Chat.cancel(ref)
  end

  @doc "Returns the observable state of a conversation."
  @spec status(Ref.t() | String.t() | nil) :: {:ok, map()} | {:error, term()}
  def status(target \\ nil) do
    with {:ok, ref} <- resolve(target), do: Chat.status(ref)
  end

  @doc "Prints a health report for one gateway, or for all of them."
  @spec doctor(atom() | nil) :: :ok
  defdelegate doctor(gateway \\ nil), to: Doctor, as: :report

  @spec ensure_current() :: {:ok, Ref.t()} | {:error, term()}
  defp ensure_current do
    case current() do
      %Ref{} = ref ->
        {:ok, ref}

      nil ->
        with {:ok, ref} <- Console.open(nil, []) do
          _current = put_current(ref)
          {:ok, ref}
        end
    end
  end

  @spec resolve(Ref.t() | String.t() | nil) :: {:ok, Ref.t()} | {:error, term()}
  defp resolve(nil), do: ensure_current()
  defp resolve(target), do: open(target)

  @spec open(module() | Ref.t() | String.t()) :: {:ok, Ref.t()} | {:error, term()}
  defp open(%Ref{} = ref), do: Gateway.open(ref.gateway, ref)
  defp open(agent) when is_atom(agent), do: Chat.open(agent)

  defp open(target) when is_binary(target) do
    case resolve_gateway(nil) do
      {:ok, gateway} -> Gateway.open(gateway, target)
      {:error, _reason} = error -> error
    end
  end

  @spec put_current(Ref.t()) :: Ref.t()
  defp put_current(ref) do
    Process.put(@current, ref)
    ref
  end

  @spec resolve_gateway(atom() | nil) :: {:ok, atom()} | {:error, term()}
  defp resolve_gateway(name) when is_atom(name) and not is_nil(name), do: {:ok, name}

  defp resolve_gateway(_nil) do
    case Gateway.list() do
      [only] -> {:ok, only}
      [] -> {:error, :no_beam_gateway_running}
      many -> {:error, {:ambiguous_beam_gateway, many}}
    end
  end

  @spec format_event(Event.t()) :: String.t()
  defp format_event(%Event{type: :inbound, payload: %{text: text, sender: sender}}),
    do: "#{sender} › #{text}"

  defp format_event(%Event{type: :reply, payload: %{text: text}}), do: "bot › #{text}"

  defp format_event(%Event{type: :error, payload: payload}),
    do: "  ! #{inspect(Map.get(payload, :reason))}"

  defp format_event(%Event{type: type, payload: payload}),
    do: "  · #{type} #{inspect(payload, limit: 3)}"

  @spec format_status(term()) :: String.t()
  defp format_status(:up), do: "up"
  defp format_status(:idle), do: "idle"
  defp format_status({:running, _since}), do: "running"
  defp format_status({:degraded, _reason}), do: "degraded"
  defp format_status(other), do: inspect(other)

  @spec format_time(DateTime.t() | nil) :: String.t()
  defp format_time(nil), do: "—"
  defp format_time(%DateTime{} = at), do: DateTime.to_iso8601(at)
end
