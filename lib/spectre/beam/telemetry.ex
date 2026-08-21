defmodule Spectre.Beam.Telemetry do
  @moduledoc """
  Optional `:telemetry` instrumentation for the gateway.

  `:telemetry` is not a Beam dependency. When the host installs it, Beam emits
  the events below; when it does not, every call here is a no-op check.

    * `[:spectre, :beam, :ingress, :start | :stop | :exception]` —
      one provider event entering the gateway.
    * `[:spectre, :beam, :turn, :start | :stop | :exception]` —
      one agent turn on a conversation.
    * `[:spectre, :beam, :deliver, :start | :stop | :exception]` —
      one outbound delivery, retries included.
    * `[:spectre, :beam, :conversation, :start | :stop]` —
      lifecycle of a conversation process.

  Stop measurements always carry `:duration` in native units.
  """

  @telemetry :telemetry

  @type span :: :ingress | :turn | :deliver | :conversation

  @doc """
  Runs `fun` as a telemetry span, returning its value unchanged.
  """
  @spec span(span(), map(), (-> result)) :: result when result: term()
  def span(name, metadata, fun) when is_atom(name) and is_map(metadata) and is_function(fun, 0) do
    if enabled?() do
      start = System.monotonic_time()
      execute([:spectre, :beam, name, :start], %{system_time: System.system_time()}, metadata)

      try do
        fun.()
      catch
        kind, reason ->
          execute(
            [:spectre, :beam, name, :exception],
            %{duration: System.monotonic_time() - start},
            Map.merge(metadata, %{kind: kind, reason: reason})
          )

          :erlang.raise(kind, reason, __STACKTRACE__)
      else
        result ->
          execute(
            [:spectre, :beam, name, :stop],
            %{duration: System.monotonic_time() - start},
            Map.put(metadata, :result, outcome(result))
          )

          result
      end
    else
      fun.()
    end
  end

  @doc "Emits one standalone telemetry event."
  @spec emit([atom()], map(), map()) :: :ok
  def emit(event, measurements, metadata)
      when is_list(event) and is_map(measurements) and is_map(metadata) do
    if enabled?(), do: execute(event, measurements, metadata), else: :ok
  end

  @spec execute([atom()], map(), map()) :: :ok
  defp execute(event, measurements, metadata) do
    # :telemetry is an optional host dependency, resolved at call time.
    # credo:disable-for-next-line Credo.Check.Refactor.Apply
    apply(@telemetry, :execute, [event, measurements, metadata])
    :ok
  end

  @spec outcome(term()) :: :ok | :ignore | :error
  defp outcome(:ok), do: :ok
  defp outcome({:ok, _value}), do: :ok
  defp outcome(:ignore), do: :ignore
  defp outcome({:error, _reason}), do: :error
  defp outcome(_other), do: :ok

  @spec enabled?() :: boolean()
  defp enabled?, do: Code.ensure_loaded?(@telemetry)
end
