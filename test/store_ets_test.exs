defmodule Spectre.Beam.Store.ETSTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Store.ETS

  setup context do
    name = :"beam_store_#{System.unique_integer([:positive])}"

    opts =
      [name: name, sweep_interval_ms: 60_000]
      |> Keyword.merge(Map.get(context, :store, []))

    start_supervised!({ETS, opts})
    %{store: Keyword.take(opts, [:name, :ttl_ms, :claim_ttl_ms]), name: name}
  end

  test "claims once, then reports the claim in progress", %{store: store} do
    assert :ok = ETS.claim(:key, store)
    assert :in_progress = ETS.claim(:key, store)
  end

  test "reports a completed claim as a duplicate carrying its value", %{store: store} do
    :ok = ETS.claim(:key, store)
    :ok = ETS.complete(:key, %{receipt: :sent}, store)

    assert {:duplicate, %{receipt: :sent}} = ETS.claim(:key, store)
  end

  test "a released claim can be taken again", %{store: store} do
    :ok = ETS.claim(:key, store)
    :ok = ETS.release(:key, store)

    assert :ok = ETS.claim(:key, store)
  end

  @tag store: [ttl_ms: 30]
  test "forgets a completed claim once its retention expires", %{store: store} do
    :ok = ETS.claim(:key, store)
    :ok = ETS.complete(:key, :value, store)
    assert {:duplicate, :value} = ETS.claim(:key, store)

    Process.sleep(60)
    assert :ok = ETS.claim(:key, store)
  end

  # A process that dies between claim and complete would otherwise fence its
  # key forever, and every provider redelivery of that message would be
  # rejected as in progress.
  @tag store: [claim_ttl_ms: 30]
  test "heals a claim abandoned by a crashed caller", %{store: store} do
    assert :ok = ETS.claim(:key, store)
    assert :in_progress = ETS.claim(:key, store)

    Process.sleep(60)
    assert :ok = ETS.claim(:key, store)
  end

  @tag store: [claim_ttl_ms: 20]
  test "an expired claim still has exactly one concurrent successor", %{store: store} do
    assert :ok = ETS.claim(:key, store)
    Process.sleep(40)

    owner = self()
    gate = :atomics.new(1, signed: false)

    claimers =
      for _index <- 1..20 do
        spawn(fn ->
          :atomics.add_get(gate, 1, 1)

          receive do
            :claim -> send(owner, {:claimed_after_expiry, ETS.claim(:key, store)})
          end
        end)
      end

    wait_until(fn -> :atomics.get(gate, 1) == 20 end)

    for pid <- claimers, do: send(pid, :claim)

    outcomes =
      for _index <- 1..20 do
        receive do
          {:claimed_after_expiry, result} -> result
        end
      end

    assert Enum.count(outcomes, &(&1 == :ok)) == 1
    assert Enum.count(outcomes, &(&1 == :in_progress)) == 19
  end

  @tag store: [ttl_ms: 20]
  test "sweeping drops expired entries instead of growing forever", %{store: store, name: name} do
    for index <- 1..25 do
      :ok = ETS.claim({:key, index}, store)
      :ok = ETS.complete({:key, index}, index, store)
    end

    assert ETS.size(name) == 25

    Process.sleep(40)
    assert ETS.sweep(name) == 25
    assert ETS.size(name) == 0
  end

  test "concurrent claimers on one key produce exactly one winner", %{store: store} do
    owner = self()

    for _index <- 1..20 do
      spawn(fn -> send(owner, {:claimed, ETS.claim(:contended, store)}) end)
    end

    outcomes = for _index <- 1..20, do: receive(do: ({:claimed, result} -> result))

    assert Enum.count(outcomes, &(&1 == :ok)) == 1
    assert Enum.count(outcomes, &(&1 == :in_progress)) == 19
  end

  test "reports a store that is not running instead of raising" do
    assert {:error, {:beam_store_not_started, :missing_store}} =
             ETS.claim(:key, name: :missing_store)

    assert {:error, {:beam_store_not_started, :missing_store}} =
             ETS.complete(:key, :value, name: :missing_store)

    assert {:error, {:beam_store_not_started, :missing_store}} =
             ETS.release(:key, name: :missing_store)
  end

  test "periodic sweeping and unrelated messages keep the owner alive", %{name: name} do
    send(name, :unrelated)
    send(name, :sweep)
    Process.sleep(10)
    assert Process.alive?(Process.whereis(name))
  end

  test "reset clears every entry", %{store: store, name: name} do
    :ok = ETS.claim(:key, store)
    assert ETS.size(name) == 1

    :ok = ETS.reset(name)
    assert ETS.size(name) == 0
  end

  defp wait_until(fun, attempts \\ 100)

  defp wait_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      wait_until(fun, attempts - 1)
    end
  end

  defp wait_until(_fun, 0), do: flunk("timed out waiting for concurrent claimers")
end
