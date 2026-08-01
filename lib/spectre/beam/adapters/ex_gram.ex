defmodule Spectre.Beam.Adapters.ExGram do
  @moduledoc """
  Optional adapter for both the local TDLib-based ExGram API and conventional
  ExGram update maps.

  No ExGram dependency is forced into `spectre_beam`. Configure the endpoint
  with `client:` (or `session:`) at runtime; `module:` can replace `ExGram` for
  compatible wrappers and tests.
  """

  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Adapters.Common
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Outbound

  @provider :"Elixir.ExGram"
  @capabilities [:text, :document, :location, :contact, :event]

  @impl true
  def capabilities(_opts), do: @capabilities

  @impl true
  def decode(event, opts) do
    with {:ok, message, envelope} <- unwrap(event),
         true <- Common.get(message, :from_me, false) != true,
         {:ok, message_id} <- Common.id(Common.first(message, [:id, :message_id])),
         conversation_id when not is_nil(conversation_id) <- conversation(message, envelope),
         {:ok, content} <- normalize_content(message),
         sender <- sender(message, conversation_id) do
      {:ok,
       Inbound.new(%{
         message_id: message_id,
         conversation_id: conversation_id,
         sender: sender,
         recipient: Keyword.get(opts, :recipient),
         content: content,
         authenticated?: Common.authenticated?(opts),
         occurred_at:
           message
           |> Common.first([:timestamp, :date])
           |> Common.occurred_at(),
         metadata:
           %{provider: :ex_gram}
           |> put_if(:session, envelope.session)
           |> put_if(:provider_kind, provider_kind(message))
       })}
    else
      false -> :ignore
      :ignore -> :ignore
      {:error, :missing_provider_message_id} -> :ignore
      nil -> {:error, :missing_ex_gram_conversation}
    end
  end

  @impl true
  def deliver(%Outbound{} = outbound, opts) do
    with {:ok, module} <- Common.provider_module(opts, @provider),
         {:ok, client} <- Common.client(opts),
         reply <- deliver_content(module, client, outbound, opts) do
      Common.normalize_delivery(reply, outbound)
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

  @spec unwrap(term()) :: {:ok, map(), map()} | :ignore
  defp unwrap({:ex_gram_message, session, jid, message}) when is_map(message),
    do: {:ok, message, %{session: session, jid: jid}}

  defp unwrap({:ex_gram_message, jid, message}) when is_map(message),
    do: {:ok, message, %{session: nil, jid: jid}}

  defp unwrap(event) when is_map(event) do
    case Common.get(event, :message) do
      message when is_map(message) ->
        {:ok, message, %{session: nil, jid: nil}}

      _other ->
        if(message_map?(event), do: {:ok, event, %{session: nil, jid: nil}}, else: :ignore)
    end
  end

  defp unwrap(_event), do: :ignore

  @spec message_map?(map()) :: boolean()
  defp message_map?(message) do
    not is_nil(Common.first(message, [:id, :message_id])) and
      (not is_nil(Common.first(message, [:chat_id, :jid, :chat])) or
         is_binary(Common.first(message, [:text, :caption])))
  end

  @spec conversation(map(), map()) :: term()
  defp conversation(message, envelope) do
    envelope.jid ||
      Common.first(message, [:jid, :chat_id]) ||
      message
      |> Common.get(:chat, %{})
      |> Common.get(:id)
  end

  @spec sender(map(), term()) :: term()
  defp sender(message, fallback) do
    Common.get(message, :sender_id) ||
      message
      |> Common.get(:from, %{})
      |> Common.get(:id) ||
      fallback
  end

  @spec normalize_content(map()) :: {:ok, Spectre.Beam.Content.t()} | :ignore
  defp normalize_content(message) do
    normalized = Common.get(message, :content, %{})
    kind = provider_kind(message)

    text =
      Common.first(message, [:text, :caption]) ||
        Common.first(normalized, [:text, :caption])

    data =
      case kind do
        kind when kind in [:location, :contact, :event] ->
          Common.get(message, kind) || Common.get(normalized, kind)

        kind when kind in [:media, :photo, :image, :video, :audio, :voice_note, :document] ->
          Common.get(message, :media) || Common.get(normalized, :media) || normalized

        _other ->
          nil
      end

    Common.content(kind, data, text, %{provider: :ex_gram})
  end

  @spec provider_kind(map()) :: atom()
  defp provider_kind(message) do
    normalized = Common.get(message, :content, %{})

    Common.get(message, :kind) ||
      Common.get(normalized, :kind) ||
      tdlib_kind(Common.get(normalized, :"@type")) ||
      infer_kind(message)
  end

  @spec tdlib_kind(term()) :: atom() | nil
  defp tdlib_kind("messageText"), do: :text
  defp tdlib_kind("messagePhoto"), do: :photo
  defp tdlib_kind("messageVideo"), do: :video
  defp tdlib_kind("messageAnimation"), do: :image
  defp tdlib_kind("messageDocument"), do: :document
  defp tdlib_kind("messageAudio"), do: :audio
  defp tdlib_kind("messageVoiceNote"), do: :voice_note
  defp tdlib_kind("messageLocation"), do: :location
  defp tdlib_kind("messageVenue"), do: :location
  defp tdlib_kind("messageContact"), do: :contact
  defp tdlib_kind(_type), do: nil

  @spec infer_kind(map()) :: atom()
  defp infer_kind(message) do
    cond do
      is_binary(Common.get(message, :text)) -> :text
      is_map(Common.get(message, :location)) -> :location
      is_map(Common.get(message, :contact)) -> :contact
      is_map(Common.get(message, :document)) -> :document
      true -> :unknown
    end
  end

  @spec deliver_content(module(), term(), Outbound.t(), keyword()) :: term()
  defp deliver_content(module, client, outbound, opts) do
    content = outbound.content
    send_opts = Common.send_options(outbound, opts)

    case content.type do
      :text ->
        Common.call_send(module, :send_message, [client, outbound.to, content.text], send_opts)

      :document ->
        Common.call(module, :send_document, [
          client,
          outbound.to,
          Common.source(content.data),
          send_opts
        ])

      :location ->
        Common.call(module, :send_location, [
          client,
          outbound.to,
          Common.data_value(content.data, :latitude),
          Common.data_value(content.data, :longitude),
          send_opts
        ])

      :contact ->
        Common.call(module, :send_contact, [
          client,
          outbound.to,
          Common.data_value(content.data, :display_name),
          Common.data_value(content.data, :vcard),
          send_opts
        ])

      :event ->
        Common.call(module, :send_event, [
          client,
          outbound.to,
          Common.data_value(content.data, :name),
          Common.data_value(content.data, :start_time),
          send_opts
        ])

      unsupported ->
        {:error, {:unsupported_ex_gram_content, unsupported}}
    end
  end

  @spec put_if(map(), atom(), term()) :: map()
  defp put_if(map, _key, nil), do: map
  defp put_if(map, key, value), do: Map.put(map, key, value)
end
