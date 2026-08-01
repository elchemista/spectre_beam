defmodule Spectre.Beam.Adapters.Common do
  @moduledoc false

  alias Spectre.Beam.Content
  alias Spectre.Beam.Outbound
  alias Spectre.Beam.Receipt

  @adapter_option_keys [
    :authenticated?,
    :await_ack,
    :client,
    :module,
    :recipient,
    :send_opts,
    :session,
    :timeout
  ]

  @spec get(term(), atom(), term()) :: term()
  def get(value, key, default \\ nil)

  def get(value, key, default) when is_map(value) and is_atom(key) do
    case Map.fetch(value, key) do
      {:ok, found} -> found
      :error -> Map.get(value, Atom.to_string(key), default)
    end
  end

  def get(_value, _key, default), do: default

  @spec first(term(), [atom()]) :: term()
  def first(value, keys) do
    Enum.find_value(keys, &get(value, &1))
  end

  @spec id(term()) :: {:ok, String.t()} | {:error, :missing_provider_message_id}
  def id(value) when is_binary(value) and value != "", do: {:ok, value}
  def id(value) when is_integer(value), do: {:ok, Integer.to_string(value)}
  def id(value) when is_atom(value) and not is_nil(value), do: {:ok, Atom.to_string(value)}
  def id(_value), do: {:error, :missing_provider_message_id}

  @spec occurred_at(term()) :: DateTime.t() | nil
  def occurred_at(%DateTime{} = occurred_at), do: occurred_at

  def occurred_at(%NaiveDateTime{} = occurred_at) do
    DateTime.from_naive!(occurred_at, "Etc/UTC")
  end

  def occurred_at(value) when is_integer(value) do
    unit = if abs(value) > 9_999_999_999, do: :millisecond, else: :second

    case DateTime.from_unix(value, unit) do
      {:ok, occurred_at} -> occurred_at
      {:error, _reason} -> nil
    end
  end

  def occurred_at(_value), do: nil

  @spec authenticated?(keyword()) :: boolean()
  def authenticated?(opts), do: Keyword.get(opts, :authenticated?, false) == true

  @spec client(keyword()) :: {:ok, term()} | {:error, :missing_beam_adapter_client}
  def client(opts) do
    case Keyword.get(opts, :client, Keyword.get(opts, :session)) do
      nil -> {:error, :missing_beam_adapter_client}
      client -> {:ok, client}
    end
  end

  @spec provider_module(keyword(), module()) :: {:ok, module()} | {:error, term()}
  def provider_module(opts, default) do
    module = Keyword.get(opts, :module, default)

    if is_atom(module) and not is_nil(module),
      do: {:ok, module},
      else: {:error, {:invalid_beam_provider_module, module}}
  end

  @spec call(module(), atom(), list()) :: term()
  def call(module, function, args) do
    cond do
      not Code.ensure_loaded?(module) ->
        {:error, {:beam_provider_not_loaded, module}}

      not function_exported?(module, function, length(args)) ->
        {:error, {:beam_provider_callback_missing, module, function, length(args)}}

      true ->
        apply(module, function, args)
    end
  end

  @spec content(atom(), term(), String.t() | nil, map()) ::
          {:ok, Content.t()} | :ignore
  def content(kind, data, text, metadata \\ %{})

  def content(kind, _data, text, metadata)
      when kind in [:text, :message_text] and is_binary(text) do
    {:ok, Content.text(text, metadata: metadata)}
  end

  def content(kind, _data, text, metadata)
      when kind in [:protocol, :unsupported, :unknown] do
    if is_binary(text),
      do: {:ok, Content.new(%{type: :text, text: text, metadata: metadata})},
      else: :ignore
  end

  def content(kind, data, text, metadata) when is_atom(kind) and not is_nil(kind) do
    type = normalize_type(kind, data)
    {:ok, Content.new(%{type: type, text: text, data: data, metadata: metadata})}
  end

  def content(_kind, _data, text, metadata) when is_binary(text),
    do: {:ok, Content.text(text, metadata: metadata)}

  def content(_kind, _data, _text, _metadata), do: :ignore

  @doc """
  Calls a provider send function, appending `send_opts` only when the provider
  exports the wider arity. Keeps compatibility with providers that expose only
  the plain `fun(client, to, value)` shape.
  """
  @spec call_send(module(), atom(), list(), keyword()) :: term()
  def call_send(module, function, args, send_opts) do
    if send_opts != [] and Code.ensure_loaded?(module) and
         function_exported?(module, function, length(args) + 1) do
      apply(module, function, args ++ [send_opts])
    else
      call(module, function, args)
    end
  end

  @spec typing(module(), keyword(), term(), boolean()) :: :ok | {:error, term()}
  def typing(module, opts, to, composing?) do
    with {:ok, provider} <- provider_module(opts, module),
         {:ok, client} <- client(opts) do
      provider
      |> call(:send_typing, [client, to, composing?])
      |> normalize_lifecycle_reply()
    end
  end

  @spec send_options(Outbound.t(), keyword()) :: keyword()
  def send_options(%Outbound{} = outbound, adapter_opts) do
    data_opts =
      case outbound.content.data do
        data when is_map(data) -> get(data, :opts, [])
        _other -> []
      end

    configured =
      adapter_opts
      |> Keyword.get(:send_opts, [])
      |> valid_keyword()

    configured
    |> Keyword.merge(valid_keyword(data_opts))
    |> maybe_put_new(:reply_to, outbound.reply_to)
  end

  @spec data_value(term(), atom(), term()) :: term()
  def data_value(data, key, default \\ nil), do: get(data, key, default)

  @spec source(term()) :: term()
  def source(data) when is_map(data) do
    first(data, [:source, :document, :media, :file, :ref]) || data
  end

  def source(data), do: data

  @spec normalize_delivery(term(), Outbound.t(), atom()) ::
          {:ok, Receipt.t()} | {:error, term()}
  def normalize_delivery(reply, outbound, dispatch \\ :synchronous)

  def normalize_delivery(:ok, outbound, _dispatch) do
    {:ok, Receipt.accepted(outbound, metadata: %{dispatch: :asynchronous})}
  end

  def normalize_delivery({:ok, provider_message_id}, outbound, dispatch) do
    {:ok,
     Receipt.accepted(outbound,
       provider_message_id: provider_message_id,
       metadata: %{dispatch: dispatch}
     )}
  end

  def normalize_delivery({:ok, _client, provider_message_id}, outbound, dispatch) do
    {:ok,
     Receipt.accepted(outbound,
       provider_message_id: provider_message_id,
       metadata: %{dispatch: dispatch}
     )}
  end

  def normalize_delivery({:error, reason}, _outbound, _dispatch), do: {:error, reason}
  def normalize_delivery({:error, reason, _client}, _outbound, _dispatch), do: {:error, reason}

  def normalize_delivery(other, _outbound, _dispatch),
    do: {:error, {:invalid_beam_provider_reply, other}}

  @spec normalize_lifecycle_reply(term()) :: :ok | {:error, term()}
  def normalize_lifecycle_reply(:ok), do: :ok
  def normalize_lifecycle_reply({:ok, _value}), do: :ok
  def normalize_lifecycle_reply({:error, _reason} = error), do: error

  def normalize_lifecycle_reply(other),
    do: {:error, {:invalid_beam_provider_lifecycle_reply, other}}

  @spec provider_options(keyword()) :: keyword()
  def provider_options(opts), do: Keyword.drop(opts, @adapter_option_keys)

  @spec normalize_type(atom(), term()) :: atom()
  defp normalize_type(kind, data) when kind in [:media, :photo] do
    case data_value(data, :type, data_value(data, :media_type)) do
      :photo -> :image
      :image -> :image
      :video -> :video
      :audio -> :audio
      :voice_note -> :audio
      :document -> :document
      _other -> if(kind == :photo, do: :image, else: :document)
    end
  end

  defp normalize_type(:message_document, _data), do: :document
  defp normalize_type(:message_photo, _data), do: :image
  defp normalize_type(:voice_note, _data), do: :audio
  defp normalize_type(kind, _data), do: kind

  @spec valid_keyword(term()) :: keyword()
  defp valid_keyword(value) when is_list(value) do
    if Keyword.keyword?(value), do: value, else: []
  end

  defp valid_keyword(_value), do: []

  @spec maybe_put_new(keyword(), atom(), term()) :: keyword()
  defp maybe_put_new(opts, _key, nil), do: opts
  defp maybe_put_new(opts, key, value), do: Keyword.put_new(opts, key, value)
end
