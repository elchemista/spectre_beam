defmodule Spectre.Beam.Socket.Protocol do
  @moduledoc """
  Request handling for the local control socket, independent of transport.

  Keeping the operations pure makes them testable without opening a socket,
  and lets the same vocabulary back an HTTP or stdio surface later.

  A request is a map with an `"op"`, an optional correlation `"id"`, and the
  operation's arguments. Replies mirror the id; pushed events arrive as
  `%{"event" => ...}` frames with no id.

      %{"id" => 1, "op" => "send", "ref" => "console:42", "text" => "ciao"}
      %{"id" => 1, "ok" => true, "data" => %{"ref" => "console:42"}}
  """

  alias Spectre.Beam.Chat
  alias Spectre.Beam.Doctor
  alias Spectre.Beam.Event
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Ref

  @type state :: %{gateway: atom(), subscriptions: MapSet.t(String.t())}

  @default_ask_timeout :timer.seconds(60)

  @doc "Returns the initial state of one connection."
  @spec init(atom()) :: state()
  def init(gateway), do: %{gateway: gateway, subscriptions: MapSet.new()}

  @doc "Handles one decoded request, returning the reply and the next state."
  @spec handle(map(), state()) :: {map(), state()}
  def handle(request, state) when is_map(request) do
    id = Map.get(request, "id")

    case Map.get(request, "op") do
      op when is_binary(op) ->
        {result, state} = dispatch(op, request, state)
        {reply(id, result), state}

      other ->
        {reply(id, {:error, {:invalid_beam_socket_op, other}}), state}
    end
  end

  @doc "Wraps a bus event into a pushed frame."
  @spec event_frame(Event.t()) :: map()
  def event_frame(%Event{} = event), do: %{"event" => Event.to_map(event)}

  @doc "Returns true when the connection asked to observe this event."
  @spec subscribed?(state(), Event.t()) :: boolean()
  def subscribed?(state, %Event{ref: ref}),
    do: MapSet.member?(state.subscriptions, Ref.slug(ref))

  @spec dispatch(String.t(), map(), state()) :: {term(), state()}
  defp dispatch("hello", _request, state) do
    {{:ok,
      %{
        "gateway" => to_string(state.gateway),
        "gateways" => Enum.map(Gateway.list(), &to_string/1),
        "version" => Spectre.Beam.version(),
        "events" => Enum.map(Event.types(), &to_string/1)
      }}, state}
  end

  defp dispatch("endpoints", _request, state) do
    endpoints =
      state.gateway
      |> Gateway.endpoints()
      |> Enum.map(fn status ->
        %{
          "endpoint" => to_string(status.endpoint),
          "status" => inspect(status.status),
          "ingress" => to_string(Map.get(status, :ingress, :none)),
          "events" => Map.get(status, :events, 0)
        }
      end)

    {{:ok, %{"endpoints" => endpoints}}, state}
  end

  defp dispatch("conversations", _request, state) do
    slugs = state.gateway |> Gateway.conversations() |> Enum.map(&Ref.slug/1)
    {{:ok, %{"conversations" => slugs}}, state}
  end

  defp dispatch("open", request, state) do
    with_ref(request, state, fn ref -> {{:ok, %{"ref" => Ref.slug(ref)}}, state} end)
  end

  defp dispatch("send", request, state) do
    with_message(request, state, fn ref, text ->
      case Chat.send(ref, text, sender: sender(request)) do
        {:ok, ^ref} -> {{:ok, %{"ref" => Ref.slug(ref), "accepted" => true}}, state}
        {:duplicate, _ref} -> {{:ok, %{"ref" => Ref.slug(ref), "duplicate" => true}}, state}
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  defp dispatch("ask", request, state) do
    with_message(request, state, fn ref, text ->
      case request_timeout(request) do
        {:ok, timeout} -> ask(ref, text, timeout, sender(request), state)
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  defp dispatch("push", request, state) do
    with_message(request, state, fn ref, text ->
      case Chat.push(ref, text) do
        {:ok, ^ref} -> {{:ok, %{"ref" => Ref.slug(ref), "queued" => true}}, state}
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  defp dispatch("subscribe", request, state) do
    with_ref(request, state, fn ref ->
      slug = Ref.slug(ref)
      :ok = Chat.subscribe(ref)

      {{:ok, %{"ref" => slug, "subscribed" => true}},
       %{state | subscriptions: MapSet.put(state.subscriptions, slug)}}
    end)
  end

  defp dispatch("unsubscribe", request, state) do
    with_ref(request, state, fn ref ->
      slug = Ref.slug(ref)
      :ok = Chat.unsubscribe(ref)

      {{:ok, %{"ref" => slug, "subscribed" => false}},
       %{state | subscriptions: MapSet.delete(state.subscriptions, slug)}}
    end)
  end

  defp dispatch("history", request, state) do
    with_ref(request, state, fn ref ->
      opts =
        []
        |> put_opt(:after, Map.get(request, "after"))
        |> put_opt(:limit, Map.get(request, "limit"))

      events = ref |> Chat.history(opts) |> Enum.map(&Event.to_map/1)
      {{:ok, %{"ref" => Ref.slug(ref), "events" => events}}, state}
    end)
  end

  defp dispatch("status", request, state) do
    with_ref(request, state, fn ref ->
      case Chat.status(ref) do
        {:ok, status} -> {{:ok, status_payload(ref, status)}, state}
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  defp dispatch("cancel", request, state) do
    with_ref(request, state, fn ref ->
      :ok = Chat.cancel(ref)
      {{:ok, %{"ref" => Ref.slug(ref), "cancelled" => true}}, state}
    end)
  end

  defp dispatch("close", request, state) do
    with_ref(request, state, fn ref ->
      :ok = Chat.close(ref)
      {{:ok, %{"ref" => Ref.slug(ref), "closed" => true}}, state}
    end)
  end

  defp dispatch("doctor", _request, state) do
    checks =
      state.gateway
      |> Doctor.run()
      |> Enum.map(fn check ->
        %{
          "scope" => scope(check.scope),
          "check" => to_string(check.check),
          "status" => to_string(check.status),
          "detail" => check.detail
        }
      end)

    {{:ok, %{"checks" => checks, "verdict" => to_string(verdict(checks))}}, state}
  end

  defp dispatch(op, _request, state), do: {{:error, {:unknown_beam_socket_op, op}}, state}

  @spec ask(Ref.t(), String.t(), non_neg_integer(), String.t(), state()) :: {term(), state()}
  defp ask(ref, text, timeout, sender, state) do
    case Chat.ask(ref, text, timeout: timeout, sender: sender) do
      {:ok, reply} -> {{:ok, %{"ref" => Ref.slug(ref), "text" => reply}}, state}
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  # Every addressed operation shares the same two failure modes, so they are
  # resolved once here instead of in ten near-identical `with` blocks.
  @spec with_ref(map(), state(), (Ref.t() -> {term(), state()})) :: {term(), state()}
  defp with_ref(request, state, fun) do
    case resolve(request, state) do
      {:ok, ref} -> fun.(ref)
      {:error, _reason} = error -> {error, state}
    end
  end

  @spec with_message(map(), state(), (Ref.t(), String.t() -> {term(), state()})) ::
          {term(), state()}
  defp with_message(request, state, fun) do
    with_ref(request, state, fn ref ->
      case fetch_text(request) do
        {:ok, text} -> fun.(ref, text)
        {:error, _reason} = error -> {error, state}
      end
    end)
  end

  @spec status_payload(Ref.t(), map()) :: map()
  defp status_payload(ref, status) do
    %{
      "ref" => Ref.slug(ref),
      "status" => inspect(status.status),
      "turns" => status.turns,
      "pending" => status.pending,
      "seq" => status.seq
    }
  end

  @spec resolve(map(), state()) :: {:ok, Ref.t()} | {:error, term()}
  defp resolve(request, state) do
    case Map.get(request, "ref") do
      slug when is_binary(slug) -> Gateway.open(state.gateway, slug)
      nil -> {:error, :beam_socket_ref_required}
      other -> {:error, {:invalid_beam_ref, other}}
    end
  end

  @spec fetch_text(map()) :: {:ok, String.t()} | {:error, term()}
  defp fetch_text(request) do
    case Map.get(request, "text") do
      text when is_binary(text) -> {:ok, text}
      other -> {:error, {:invalid_beam_socket_text, other}}
    end
  end

  @spec sender(map()) :: String.t()
  defp sender(request) do
    case Map.get(request, "sender") do
      sender when is_binary(sender) -> sender
      _absent -> "socket"
    end
  end

  @spec request_timeout(map()) :: {:ok, non_neg_integer()} | {:error, term()}
  defp request_timeout(request) do
    case Map.get(request, "timeout", @default_ask_timeout) do
      timeout when is_integer(timeout) and timeout >= 0 -> {:ok, timeout}
      timeout -> {:error, {:invalid_beam_timeout, timeout}}
    end
  end

  @spec put_opt(keyword(), atom(), term()) :: keyword()
  defp put_opt(opts, _key, nil), do: opts
  defp put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  @spec reply(term(), term()) :: map()
  defp reply(id, {:ok, data}), do: %{"id" => id, "ok" => true, "data" => data}
  defp reply(id, {:error, reason}), do: %{"id" => id, "ok" => false, "error" => inspect(reason)}
  defp reply(id, other), do: %{"id" => id, "ok" => false, "error" => inspect(other)}

  @spec scope(term()) :: String.t()
  defp scope({gateway, endpoint}), do: "#{gateway}/#{endpoint}"
  defp scope(value), do: inspect(value)

  @spec verdict([map()]) :: atom()
  defp verdict(checks) do
    cond do
      Enum.any?(checks, &(&1["status"] == "error")) -> :error
      Enum.any?(checks, &(&1["status"] == "warn")) -> :warn
      true -> :ok
    end
  end
end
