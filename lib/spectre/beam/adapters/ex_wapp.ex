defmodule Spectre.Beam.Adapters.ExWapp do
  @moduledoc """
  Optional adapter for ExWapp's normalized message and send APIs.

  Set `await_ack: true` for synchronous text sends when the configured client
  is a session pid. An acknowledgement timeout is reported as an ambiguous
  Beam outcome so the idempotency claim is intentionally retained.

  An awaited send that carries send options (e.g. a stable
  `retry_message_id:`) needs a provider function able to accept them. When the
  provider module has no such arity, pass `send_await:` in the adapter
  options — a `(client, to, text, timeout, send_opts)` function the host
  builds on its provider's own API — and the adapter dispatches to it instead
  of guessing.
  """

  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Adapters.Common
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Outbound

  @provider :"Elixir.ExWapp"
  @capabilities [:text, :document, :location, :contact, :event]

  @impl true
  def capabilities(_opts), do: @capabilities

  @impl true
  def decode(event, opts) do
    with {:ok, jid, message, session} <- unwrap(event),
         true <- Common.get(message, :from_me, false) != true,
         {:ok, message_id} <- Common.id(Common.get(message, :id)),
         {:ok, content} <- normalize_content(message),
         conversation_id when not is_nil(conversation_id) <-
           Common.first(message, [:conversation_id, :jid]) || jid do
      {:ok,
       Inbound.new(%{
         message_id: message_id,
         conversation_id: conversation_id,
         sender: Common.get(message, :participant) || jid,
         recipient: Keyword.get(opts, :recipient),
         content: content,
         authenticated?: Common.authenticated?(opts),
         occurred_at:
           message
           |> Common.get(:timestamp)
           |> Common.occurred_at(),
         metadata:
           %{provider: :ex_wapp}
           |> put_if(:session, session)
           |> put_if(:provider_kind, provider_kind(message))
       })}
    else
      false -> :ignore
      :ignore -> :ignore
      {:error, :missing_provider_message_id} -> :ignore
      nil -> {:error, :missing_ex_wapp_conversation}
    end
  end

  @impl true
  def deliver(%Outbound{} = outbound, opts) do
    with {:ok, module} <- Common.provider_module(opts, @provider),
         {:ok, client} <- Common.client(opts),
         reply <- deliver_content(module, client, outbound, opts) do
      normalize_delivery(reply, outbound, opts)
    end
  end

  @impl true
  def typing(to, composing?, opts), do: Common.typing(@provider, opts, to, composing?)

  @impl true
  def subscribe(opts) do
    with {:ok, module} <- Common.provider_module(opts, @provider),
         {:ok, client} <- Common.client(opts) do
      module
      |> Common.call(:subscribe, [client])
      |> Common.normalize_lifecycle_reply()
    end
  end

  @impl true
  def unsubscribe(opts) do
    with {:ok, module} <- Common.provider_module(opts, @provider),
         {:ok, client} <- Common.client(opts) do
      module
      |> Common.call(:unsubscribe, [client])
      |> Common.normalize_lifecycle_reply()
    end
  end

  @spec unwrap(term()) :: {:ok, term(), map(), term()} | :ignore
  defp unwrap({:ex_wapp_message, session, jid, message}) when is_map(message),
    do: {:ok, jid, message, session}

  defp unwrap({:ex_wapp_message, jid, message}) when is_map(message),
    do: {:ok, jid, message, nil}

  defp unwrap(message) when is_map(message) do
    jid = Common.first(message, [:jid, :conversation_id])
    if is_nil(jid), do: :ignore, else: {:ok, jid, message, nil}
  end

  defp unwrap(_event), do: :ignore

  @spec normalize_content(map()) :: {:ok, Spectre.Beam.Content.t()} | :ignore
  defp normalize_content(message) do
    normalized = Common.get(message, :content, %{})
    kind = provider_kind(message)
    text = Common.get(normalized, :text) || Common.get(message, :text)

    data =
      case kind do
        :media ->
          Common.get(normalized, :media) ||
            Common.get(message, :media) ||
            normalized

        kind when kind in [:location, :contact, :event] ->
          Common.get(normalized, kind) || Common.get(message, kind)

        _other ->
          nil
      end

    Common.content(kind, data, text, %{provider: :ex_wapp})
  end

  @spec provider_kind(map()) :: atom()
  defp provider_kind(message) do
    normalized = Common.get(message, :content, %{})

    Common.get(normalized, :kind) ||
      cond do
        not is_nil(Common.get(message, :media)) -> :media
        is_map(Common.get(message, :location)) -> :location
        is_map(Common.get(message, :contact)) -> :contact
        is_map(Common.get(message, :event)) -> :event
        is_binary(Common.get(message, :text)) -> :text
        true -> :unknown
      end
  end

  @spec deliver_content(module(), term(), Outbound.t(), keyword()) :: term()
  defp deliver_content(module, client, outbound, opts) do
    content = outbound.content
    send_opts = Common.send_options(outbound, opts)
    await_ack? = Keyword.get(opts, :await_ack, false) == true

    case {content.type, await_ack? and is_pid(client)} do
      {:text, true} ->
        awaited_text(
          module,
          client,
          outbound.to,
          content.text,
          Keyword.get(opts, :timeout, 15_000),
          send_opts,
          opts
        )

      {:text, false} ->
        plain_text(module, client, outbound.to, content.text, send_opts)

      {:document, _await_ack?} ->
        Common.call(module, :send_document, [
          client,
          outbound.to,
          Common.source(content.data),
          send_opts
        ])

      {:location, _await_ack?} ->
        Common.call(module, :send_location, [
          client,
          outbound.to,
          Common.data_value(content.data, :latitude),
          Common.data_value(content.data, :longitude),
          send_opts
        ])

      {:contact, _await_ack?} ->
        Common.call(module, :send_contact, [
          client,
          outbound.to,
          Common.data_value(content.data, :display_name),
          Common.data_value(content.data, :vcard),
          send_opts
        ])

      {:event, _await_ack?} ->
        Common.call(module, :send_event, [
          client,
          outbound.to,
          Common.data_value(content.data, :name),
          Common.data_value(content.data, :start_time),
          send_opts
        ])

      {unsupported, _await_ack?} ->
        {:error, {:unsupported_ex_wapp_content, unsupported}}
    end
  end

  @spec plain_text(module(), term(), term(), String.t(), keyword()) :: term()
  defp plain_text(module, client, to, text, send_opts),
    do: Common.call_send(module, :send_message, [client, to, text], send_opts)

  # The awaited send must never drop send options silently: a retried delivery
  # that loses `retry_message_id:` turns one message into two for the
  # recipient. A host whose provider version has no wide awaited arity passes
  # `send_await:` — a 5-arity function built on the provider's own API — in
  # the adapter options.
  @spec awaited_text(module(), term(), term(), String.t(), timeout(), keyword(), keyword()) ::
          term()
  defp awaited_text(module, client, to, text, timeout, send_opts, opts) do
    case Keyword.get(opts, :send_await) do
      fun when is_function(fun, 5) ->
        fun.(client, to, text, timeout, send_opts)

      _no_fun when send_opts == [] ->
        Common.call(module, :send_message_await, [client, to, text, timeout])

      _no_fun ->
        Common.call(module, :send_message_await, [client, to, text, timeout, send_opts])
    end
  end

  @spec normalize_delivery(term(), Outbound.t(), keyword()) ::
          {:ok, Spectre.Beam.Receipt.t()} | {:error, term()}
  defp normalize_delivery(reply, outbound, opts) do
    if reply == {:error, :ack_timeout} and Keyword.get(opts, :await_ack, false) do
      {:error, {:ambiguous, :ack_timeout}}
    else
      dispatch =
        if Keyword.get(opts, :await_ack, false), do: :acknowledged, else: :synchronous

      Common.normalize_delivery(reply, outbound, dispatch)
    end
  end

  @spec put_if(map(), atom(), term()) :: map()
  defp put_if(map, _key, nil), do: map
  defp put_if(map, key, value), do: Map.put(map, key, value)
end
