defmodule Spectre.Beam.Doctor do
  @moduledoc """
  Reports whether a gateway is actually able to do its job.

  Most gateway failures are configuration failures that only surface on the
  first real message: an adapter module that is not loaded, a provider client
  that never resolved, a store that is not running, an outbox backing up. The
  doctor asks those questions directly.

      iex> Spectre.Beam.Doctor.run()
  """

  alias Spectre.Beam.Endpoint.Server, as: EndpointServer
  alias Spectre.Beam.Gateway
  alias Spectre.Beam.Gateway.Spec
  alias Spectre.Beam.Outbox
  alias Spectre.Beam.Runtime

  @type status :: :ok | :warn | :error

  @type check :: %{
          scope: atom() | {atom(), term()},
          check: atom(),
          status: status(),
          detail: String.t()
        }

  @runtime_processes [
    Spectre.Beam.Registry,
    Spectre.Beam.Bus.Registry,
    Spectre.Beam.Sequence,
    Spectre.Beam.TaskSupervisor,
    Spectre.Beam.Store,
    Spectre.Beam.Throttle.Local
  ]

  @doc """
  Runs every check, for one gateway or for all of them.
  """
  @spec run(atom() | nil) :: [check()]
  def run(gateway \\ nil) do
    runtime_checks() ++ Enum.flat_map(gateways(gateway), &gateway_checks/1)
  end

  @doc "Returns the worst status in a report."
  @spec verdict([check()]) :: status()
  def verdict(checks) do
    cond do
      Enum.any?(checks, &(&1.status == :error)) -> :error
      Enum.any?(checks, &(&1.status == :warn)) -> :warn
      true -> :ok
    end
  end

  @doc "Prints a report to the console."
  @spec report(atom() | nil) :: :ok
  def report(gateway \\ nil) do
    checks = run(gateway)
    scope_width = column_width(checks, &scope_label(&1.scope))
    check_width = column_width(checks, &check_label(&1.check))

    Enum.each(checks, fn check ->
      IO.puts([
        marker(check.status),
        "  ",
        String.pad_trailing(scope_label(check.scope), scope_width),
        String.pad_trailing(check_label(check.check), check_width),
        check.detail
      ])
    end)

    IO.puts("\n#{marker(verdict(checks))}  #{length(checks)} checks")
  end

  @spec column_width([check()], (check() -> String.t())) :: pos_integer()
  defp column_width(checks, label) do
    checks
    |> Enum.map(&String.length(label.(&1)))
    |> Enum.max(fn -> 0 end)
    |> Kernel.+(2)
  end

  @spec runtime_checks() :: [check()]
  defp runtime_checks do
    process_checks =
      Enum.map(@runtime_processes, fn name ->
        if Process.whereis(name),
          do: check(:runtime, name, :ok, "running"),
          else: check(:runtime, name, :error, "not started — is :spectre_beam in the tree?")
      end)

    spectre =
      if Runtime.spectre_available?(),
        do: check(:runtime, :spectre, :ok, "loaded"),
        else: check(:runtime, :spectre, :warn, "not loaded — gateways run transport-only")

    process_checks ++ [spectre]
  end

  @spec gateway_checks(atom()) :: [check()]
  defp gateway_checks(gateway) do
    case Gateway.spec(gateway) do
      {:ok, spec} ->
        [agent_check(gateway, spec), store_check(gateway, spec)] ++
          Enum.flat_map(Spec.endpoints(spec), &endpoint_checks(gateway, spec, &1))

      {:error, :not_found} ->
        [check(gateway, :gateway, :error, "not running")]
    end
  end

  @spec agent_check(atom(), Spec.t()) :: check()
  defp agent_check(gateway, %Spec{agent: nil}),
    do: check(gateway, :agent, :warn, "none configured — transport-only mode")

  defp agent_check(gateway, %Spec{agent: agent}) do
    if Code.ensure_loaded?(agent),
      do: check(gateway, :agent, :ok, inspect(agent)),
      else: check(gateway, :agent, :error, "#{inspect(agent)} is not loaded")
  end

  @spec store_check(atom(), Spec.t()) :: check()
  defp store_check(gateway, %Spec{store: nil}),
    do: check(gateway, :store, :warn, "default in-memory store keeps claims forever")

  defp store_check(gateway, %Spec{store: {module, opts}}) do
    cond do
      not Code.ensure_loaded?(module) ->
        check(gateway, :store, :error, "#{inspect(module)} is not loaded")

      not function_exported?(module, :claim, 2) ->
        check(gateway, :store, :error, "#{inspect(module)} does not implement the store contract")

      true ->
        probe_store(gateway, module, opts)
    end
  end

  @spec probe_store(atom(), module(), keyword()) :: check()
  defp probe_store(gateway, module, opts) do
    key = {:doctor, gateway, System.unique_integer([:positive])}

    case module.claim(key, opts) do
      :ok ->
        module.release(key, opts)
        check(gateway, :store, :ok, inspect(module))

      other ->
        check(gateway, :store, :error, "claim returned #{inspect(other)}")
    end
  rescue
    exception -> check(gateway, :store, :error, Exception.message(exception))
  end

  @spec endpoint_checks(atom(), Spec.t(), Spectre.Beam.Endpoint.t()) :: [check()]
  defp endpoint_checks(gateway, spec, endpoint) do
    scope = {gateway, endpoint.id}

    [
      adapter_check(scope, endpoint),
      server_check(scope, gateway, endpoint),
      outbox_check(scope, gateway, endpoint),
      channel_check(scope, spec, endpoint)
    ]
  end

  @spec adapter_check({atom(), term()}, Spectre.Beam.Endpoint.t()) :: check()
  defp adapter_check(scope, endpoint) do
    cond do
      not Code.ensure_loaded?(endpoint.adapter) ->
        check(scope, :adapter, :error, "#{inspect(endpoint.adapter)} is not loaded")

      not function_exported?(endpoint.adapter, :decode, 2) ->
        check(scope, :adapter, :error, "#{inspect(endpoint.adapter)} exports no decode/2")

      not function_exported?(endpoint.adapter, :deliver, 2) ->
        check(scope, :adapter, :error, "#{inspect(endpoint.adapter)} exports no deliver/2")

      true ->
        check(scope, :adapter, :ok, inspect(endpoint.adapter))
    end
  end

  @spec server_check({atom(), term()}, atom(), Spectre.Beam.Endpoint.t()) :: check()
  defp server_check(scope, gateway, endpoint) do
    case EndpointServer.status(gateway, endpoint.id) do
      {:ok, %{status: :up} = status} ->
        check(scope, :endpoint, :ok, "up, ingress #{status.ingress}, #{status.events} events")

      {:ok, %{status: {:degraded, reason}}} ->
        check(scope, :endpoint, :warn, "degraded: #{inspect(reason)}")

      {:error, :not_found} ->
        check(scope, :endpoint, :error, "process not running")
    end
  end

  @spec outbox_check({atom(), term()}, atom(), Spectre.Beam.Endpoint.t()) :: check()
  defp outbox_check(scope, gateway, endpoint) do
    case Outbox.info(gateway, endpoint.id) do
      {:ok, %{queued: queued, max_queue: max} = info} when queued * 2 >= max ->
        check(scope, :outbox, :warn, "#{queued}/#{max} queued, #{info.failed} failed")

      {:ok, info} ->
        check(scope, :outbox, :ok, "#{info.queued} queued, #{info.delivered} delivered")

      {:error, :not_found} ->
        check(scope, :outbox, :error, "process not running")
    end
  end

  @spec channel_check({atom(), term()}, Spec.t(), Spectre.Beam.Endpoint.t()) :: check()
  defp channel_check(scope, spec, endpoint) do
    case Spec.channel(spec, endpoint.id) do
      {:ok, channel} -> scope_check(scope, spec, channel)
      {:error, reason} -> check(scope, :scope, :error, inspect(reason))
    end
  end

  @spec scope_check({atom(), term()}, Spec.t(), Spec.channel()) :: check()
  defp scope_check(scope, spec, channel) do
    cond do
      channel.scope == :instance and is_nil(spec.supervisor) ->
        check(scope, :scope, :error, "instance scope needs a :supervisor")

      channel.session? and is_nil(spec.supervisor) ->
        check(scope, :scope, :warn, "session mode needs a :supervisor, falling back stateless")

      true ->
        check(scope, :scope, :ok, Atom.to_string(channel.scope))
    end
  end

  @spec gateways(atom() | nil) :: [atom()]
  defp gateways(nil), do: Gateway.list()
  defp gateways(gateway), do: [gateway]

  @spec check(atom() | {atom(), term()}, atom(), status(), String.t()) :: check()
  defp check(scope, name, status, detail),
    do: %{scope: scope, check: name, status: status, detail: detail}

  @spec marker(status()) :: String.t()
  defp marker(:ok), do: "ok  "
  defp marker(:warn), do: "warn"
  defp marker(:error), do: "FAIL"

  @spec scope_label(atom() | {atom(), term()}) :: String.t()
  defp scope_label({gateway, endpoint}), do: "#{inspect(gateway)}/#{endpoint}"
  defp scope_label(scope) when is_atom(scope), do: inspect(scope)

  @spec check_label(atom()) :: String.t()
  defp check_label(check) do
    case Atom.to_string(check) do
      "Elixir." <> module -> module
      name -> name
    end
  end
end
