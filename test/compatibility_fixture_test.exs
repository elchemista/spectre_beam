defmodule Spectre.Beam.CompatibilityFixtureTest do
  use ExUnit.Case, async: true

  alias Spectre.Beam.Content
  alias Spectre.Beam.Exchange
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Receipt
  alias Spectre.Input
  alias Spectre.Input.Source
  alias Spectre.Result
  alias Spectre.Route
  alias Spectre.Run.Boundary
  alias Spectre.Run.Ref
  alias Spectre.State
  alias Spectre.Turn

  @fixture Path.expand(
             "fixtures/compatibility/0.1.6/beam-exchange.term.base64",
             __DIR__
           )

  @fixture_atoms [
    :accepted,
    :agent,
    :beam,
    :boundary,
    :complete,
    :content_type,
    :cursor,
    :fixture,
    :id,
    :language,
    :lifecycle,
    :provider,
    :ref,
    :recovered?,
    :regex,
    :reply,
    :reply?,
    :reply_ready,
    :revision,
    :run,
    :run_ref,
    :run_status,
    :status,
    :step_id,
    :telegram,
    :text,
    :trace_id,
    :type
  ]

  @fixture_modules [
    Exchange,
    Inbound,
    Content,
    Receipt,
    Input,
    Source,
    Result,
    Route,
    Boundary,
    Ref,
    State,
    Turn,
    DateTime,
    Calendar.ISO,
    Spectre.Beam
  ]

  test "the permanent Beam exchange restores as a completed deduplication value" do
    _loaded_atoms = @fixture_atoms
    exchange = read_fixture()

    assert %Exchange{
             duplicate?: false,
             metadata: %{fixture: "0.1.6", recovered?: true},
             inbound:
               %Inbound{
                 endpoint: :telegram,
                 channel_type: :telegram,
                 message_id: "beam-message-0.1.6",
                 conversation_id: "beam-chat-42",
                 sender: "external-user-7",
                 authenticated?: true,
                 content: %Content{type: :text, text: "status"}
               } = inbound,
             input: %Input{} = input,
             turn:
               %Turn{
                 agent: Spectre.Beam,
                 ref:
                   %Ref{
                     run_id: "run-beam-0.1.6",
                     revision: 1,
                     kind: :reply,
                     boundary_id: "reply-beam-0.1.6"
                   } = ref,
                 boundary: %Boundary{kind: :reply, output: "All systems operational."},
                 observable: {:reply, "All systems operational.", ref}
               } = turn,
             receipt: %Receipt{
               endpoint: :telegram,
               outbound_id: "beam-reply-0.1.6",
               provider_message_id: "telegram-ack-42",
               status: :accepted,
               occurred_at: ~U[2026-07-31 08:45:00Z]
             }
           } = exchange

    assert Inbound.key(inbound) == {:telegram, "beam-message-0.1.6"}
    assert input.raw == inbound
    assert input.source.kind == :beam
    assert input.source.mount == :telegram
    assert turn.result.state.state_version == 5
    assert turn.result.state.revision == 1
  end

  defp read_fixture do
    Enum.each(@fixture_modules, &Code.ensure_loaded!/1)

    @fixture
    |> File.read!()
    |> String.replace(~r/\s+/, "")
    |> Base.decode64!()
    # Exchange has no public wire decoder; this reviewed fixture witnesses the
    # trusted in-application handoff shape only.
    |> :erlang.binary_to_term()
  end
end
