unless Code.ensure_loaded?(:telemetry) do
  defmodule :telemetry do
    @moduledoc false

    def execute(event, measurements, metadata) do
      if owner = :persistent_term.get({__MODULE__, :test_owner}, nil) do
        send(owner, {:telemetry, event, measurements, metadata})
      end

      :ok
    end
  end
end

defmodule Spectre.Beam.TelemetryTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Telemetry

  setup do
    key = {:telemetry, :test_owner}
    :persistent_term.put(key, self())
    on_exit(fn -> :persistent_term.erase(key) end)
    :ok
  end

  test "emits standalone events" do
    assert :ok = Telemetry.emit([:spectre, :beam, :custom], %{count: 1}, %{gateway: :demo})

    assert_receive {:telemetry, [:spectre, :beam, :custom], %{count: 1}, %{gateway: :demo}}
  end

  test "spans successful and ignored results" do
    assert {:ok, 42} = Telemetry.span(:turn, %{ref: "local:1"}, fn -> {:ok, 42} end)

    assert_receive {:telemetry, [:spectre, :beam, :turn, :start], %{system_time: _},
                    %{ref: "local:1"}}

    assert_receive {:telemetry, [:spectre, :beam, :turn, :stop], %{duration: duration},
                    %{ref: "local:1", result: :ok}}

    assert is_integer(duration)

    assert :ignore = Telemetry.span(:ingress, %{}, fn -> :ignore end)
    assert_receive {:telemetry, [:spectre, :beam, :ingress, :stop], _, %{result: :ignore}}

    assert {:error, :offline} = Telemetry.span(:deliver, %{}, fn -> {:error, :offline} end)
    assert_receive {:telemetry, [:spectre, :beam, :deliver, :stop], _, %{result: :error}}

    assert :anything = Telemetry.span(:conversation, %{}, fn -> :anything end)
    assert_receive {:telemetry, [:spectre, :beam, :conversation, :stop], _, %{result: :ok}}
  end

  test "reports exceptions and preserves their class and stack" do
    assert_raise ArgumentError, "bad turn", fn ->
      Telemetry.span(:turn, %{gateway: :demo}, fn -> raise ArgumentError, "bad turn" end)
    end

    assert_receive {:telemetry, [:spectre, :beam, :turn, :exception], %{duration: _}, metadata}
    assert metadata.kind == :error
    assert %ArgumentError{} = metadata.reason

    assert catch_throw(Telemetry.span(:turn, %{}, fn -> throw(:stopped) end)) == :stopped
    assert_receive {:telemetry, [:spectre, :beam, :turn, :exception], _, %{kind: :throw}}
  end
end
