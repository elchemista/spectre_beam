defmodule Spectre.Beam.Identity do
  @moduledoc """
  Explicit bridge from authenticated channel principals to Agent Instances.

  Beam proves and normalizes the external identity. The Spectre core remains
  the only authority that links that identity to a canonical Subject and owns
  the resulting Instance. Conversation ids, display names, phone-number
  similarity, message content, and model output are never identity evidence.
  """

  alias Spectre.AgentRef
  alias Spectre.Beam.Inbound
  alias Spectre.ExternalIdentity
  alias Spectre.Instance.Registry, as: InstanceRegistry
  alias Spectre.Run.Value
  alias Spectre.Subject.Registry, as: SubjectRegistry

  @doc """
  Builds an opaque core identity from an authenticated Beam inbound.

  `:authenticated_at`, `:proof_ref`, and `:identity_metadata` describe the
  channel authentication event. The raw sender is used only while deriving
  the opaque core identity and is not retained by `ExternalIdentity`.
  """
  @spec external_identity(Inbound.t(), keyword()) ::
          {:ok, ExternalIdentity.t()} | {:error, term()}
  def external_identity(inbound, opts \\ [])

  def external_identity(%Inbound{} = inbound, opts) when is_list(opts) do
    with :ok <- authenticated(inbound),
         :ok <- sender_present(inbound),
         {:ok, authenticated_at} <- authenticated_at(opts),
         {:ok, metadata} <- identity_metadata(opts) do
      identity =
        ExternalIdentity.new(
          provider: :beam,
          channel: inbound.channel_type,
          endpoint: inbound.endpoint,
          principal_id: inbound.sender,
          authenticated_at: authenticated_at,
          proof_ref: Keyword.get(opts, :proof_ref),
          metadata: metadata
        )

      {:ok, identity}
    end
  rescue
    exception in [ArgumentError, KeyError] ->
      {:error, {:invalid_beam_external_identity, Exception.message(exception)}}
  end

  def external_identity(inbound, _opts),
    do: {:error, {:invalid_beam_identity_inbound, inbound}}

  @doc """
  Resolves an inbound to the unique local Instance for its linked Subject.

  The function never creates a Subject link. Bootstrap and channel-link
  confirmation must happen through `Spectre.Subject.Registry` before this
  boundary is called.
  """
  @spec resolve_instance(
          GenServer.server(),
          module() | AgentRef.t(),
          Inbound.t(),
          keyword()
        ) :: {:ok, pid()} | {:error, term()}
  def resolve_instance(supervisor, agent, inbound, opts \\ [])

  def resolve_instance(supervisor, agent, %Inbound{} = inbound, opts)
      when is_list(opts) do
    subject_registry = Keyword.get(opts, :subject_registry, SubjectRegistry)
    instance_registry = Keyword.get(opts, :instance_registry, InstanceRegistry)

    with {:ok, agent_ref} <- normalize_agent_ref(agent),
         {:ok, identity} <- external_identity(inbound, opts),
         {:ok, subject, _link} <-
           SubjectRegistry.resolve(subject_registry, agent_ref, identity) do
      supervisor
      |> InstanceRegistry.ensure_started(
        agent_ref,
        subject,
        instance_opts(opts, instance_registry)
      )
      |> normalize_instance_start()
    end
  end

  def resolve_instance(_supervisor, _agent, inbound, _opts),
    do: {:error, {:invalid_beam_identity_inbound, inbound}}

  @spec normalize_agent_ref(module() | AgentRef.t()) :: {:ok, AgentRef.t()} | {:error, term()}
  defp normalize_agent_ref(%AgentRef{} = ref) do
    case AgentRef.validate(ref) do
      :ok -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_agent_ref(agent) when is_atom(agent) and not is_nil(agent) do
    {:ok, AgentRef.new(agent)}
  rescue
    exception in ArgumentError ->
      {:error, {:invalid_beam_agent_ref, Exception.message(exception)}}
  end

  defp normalize_agent_ref(agent), do: {:error, {:invalid_beam_agent_ref, agent}}

  @spec authenticated(Inbound.t()) :: :ok | {:error, :beam_external_identity_not_authenticated}
  defp authenticated(%Inbound{authenticated?: true}), do: :ok
  defp authenticated(%Inbound{}), do: {:error, :beam_external_identity_not_authenticated}

  @spec sender_present(Inbound.t()) :: :ok | {:error, term()}
  defp sender_present(%Inbound{sender: nil}),
    do: {:error, :beam_external_identity_sender_required}

  defp sender_present(%Inbound{sender: sender}) when is_binary(sender) do
    if String.trim(sender) == "",
      do: {:error, :beam_external_identity_sender_required},
      else: :ok
  end

  defp sender_present(%Inbound{sender: sender}) do
    case Value.validate(sender, [:beam, :external_identity, :sender]) do
      :ok -> :ok
      {:error, reason} -> {:error, {:invalid_beam_external_identity_sender, reason}}
    end
  end

  @spec authenticated_at(keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  defp authenticated_at(opts) do
    case Keyword.get(opts, :authenticated_at, System.system_time(:millisecond)) do
      value when is_integer(value) and value >= 0 ->
        {:ok, value}

      %DateTime{} = value ->
        {:ok, DateTime.to_unix(value, :millisecond)}

      value ->
        {:error, {:invalid_beam_authentication_time, value}}
    end
  end

  @spec identity_metadata(keyword()) :: {:ok, map()} | {:error, term()}
  defp identity_metadata(opts) do
    case Keyword.get(opts, :identity_metadata, %{}) do
      metadata when is_map(metadata) -> {:ok, metadata}
      metadata -> {:error, {:invalid_beam_identity_metadata, metadata}}
    end
  end

  @spec instance_opts(keyword(), atom()) :: keyword()
  defp instance_opts(opts, instance_registry) do
    opts
    |> Keyword.get(:instance_opts, [])
    |> Keyword.put(:registry, instance_registry)
  end

  @spec normalize_instance_start(term()) :: {:ok, pid()} | {:error, term()}
  defp normalize_instance_start({:ok, pid}) when is_pid(pid), do: {:ok, pid}
  defp normalize_instance_start({:ok, pid, _info}) when is_pid(pid), do: {:ok, pid}
  defp normalize_instance_start({:error, _reason} = error), do: error
  defp normalize_instance_start(:ignore), do: {:error, :beam_instance_start_ignored}
end
