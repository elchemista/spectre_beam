defmodule Spectre.Beam.Identity do
  @moduledoc """
  Explicit bridge from authenticated channel principals to Spectre Instances.

  Core modules are invoked only when this boundary is used. This keeps Beam's
  dependency graph standalone while preserving Spectre's identity authority.
  """

  alias Spectre.Beam.Inbound

  @agent_ref :"Elixir.Spectre.AgentRef"
  @external_identity :"Elixir.Spectre.ExternalIdentity"
  @instance_registry :"Elixir.Spectre.Instance.Registry"
  @run_value :"Elixir.Spectre.Run.Value"
  @subject_registry :"Elixir.Spectre.Subject.Registry"

  @spec external_identity(Inbound.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def external_identity(inbound, opts \\ [])

  def external_identity(%Inbound{} = inbound, opts) when is_list(opts) do
    with :ok <- authenticated(inbound),
         :ok <- sender_present(inbound),
         {:ok, authenticated_at} <- authenticated_at(opts),
         {:ok, metadata} <- identity_metadata(opts),
         :ok <- ensure_core(@external_identity) do
      identity =
        apply(@external_identity, :new, [
          [
            provider: :beam,
            channel: inbound.channel_type,
            endpoint: inbound.endpoint,
            principal_id: inbound.sender,
            authenticated_at: authenticated_at,
            proof_ref: Keyword.get(opts, :proof_ref),
            metadata: metadata
          ]
        ])

      {:ok, identity}
    end
  rescue
    exception in [ArgumentError, KeyError] ->
      {:error, {:invalid_beam_external_identity, Exception.message(exception)}}
  end

  def external_identity(inbound, _opts),
    do: {:error, {:invalid_beam_identity_inbound, inbound}}

  @spec resolve_instance(GenServer.server(), module() | map(), Inbound.t(), keyword()) ::
          {:ok, pid()} | {:error, term()}
  def resolve_instance(supervisor, agent, inbound, opts \\ [])

  def resolve_instance(supervisor, agent, %Inbound{} = inbound, opts) when is_list(opts) do
    subject_registry_server = Keyword.get(opts, :subject_registry, @subject_registry)
    instance_registry_name = Keyword.get(opts, :instance_registry, @instance_registry)

    with {:ok, agent_ref} <- normalize_agent_ref(agent),
         {:ok, identity} <- external_identity(inbound, opts),
         :ok <- ensure_core(@subject_registry),
         {:ok, subject, _link} <-
           apply(@subject_registry, :resolve, [subject_registry_server, agent_ref, identity]),
         :ok <- ensure_core(@instance_registry) do
      @instance_registry
      |> apply(:ensure_started, [
        supervisor,
        agent_ref,
        subject,
        instance_opts(opts, instance_registry_name)
      ])
      |> normalize_instance_start()
    end
  end

  def resolve_instance(_supervisor, _agent, inbound, _opts),
    do: {:error, {:invalid_beam_identity_inbound, inbound}}

  @spec normalize_agent_ref(module() | map()) :: {:ok, map()} | {:error, term()}
  defp normalize_agent_ref(%{__struct__: @agent_ref} = ref) do
    case apply(@agent_ref, :validate, [ref]) do
      :ok -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_agent_ref(agent) when is_atom(agent) and not is_nil(agent) do
    with :ok <- ensure_core(@agent_ref) do
      {:ok, apply(@agent_ref, :new, [agent])}
    end
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
    with :ok <- ensure_core(@run_value) do
      case apply(@run_value, :validate, [sender, [:beam, :external_identity, :sender]]) do
        :ok -> :ok
        {:error, reason} -> {:error, {:invalid_beam_external_identity_sender, reason}}
      end
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

  @spec ensure_core(module()) :: :ok | {:error, :spectre_not_available}
  defp ensure_core(module) do
    if Code.ensure_loaded?(module), do: :ok, else: {:error, :spectre_not_available}
  end
end
