defmodule Spectre.Beam.RuntimeTest.Adapter do
  @behaviour Spectre.Beam.Channel

  @impl true
  def capabilities(_opts), do: MapSet.new([:text, :document])

  @impl true
  def decode(event, _opts) do
    {:ok,
     %Spectre.Beam.Inbound{
       message_id: event.message_id,
       conversation_id: event.conversation_id,
       sender: event.sender,
       recipient: "agent",
       content: Spectre.Beam.Content.text(event.text),
       authenticated?: true,
       occurred_at: ~U[2026-07-28 10:00:00Z],
       metadata: %{}
     }}
  end

  @impl true
  def deliver(outbound, opts) do
    if pid = Keyword.get(opts, :test_pid), do: send(pid, {:beam_delivered, outbound})

    {:ok,
     Spectre.Beam.Receipt.accepted(
       outbound,
       provider_message_id: "provider-#{outbound.idempotency_key}"
     )}
  end
end

defmodule Spectre.Beam.RuntimeTest.Model do
  def complete(_prompt, _opts), do: {:ok, "policy question"}
end

defmodule Spectre.Beam.RuntimeTest.Agent do
  use Spectre.Agent, prompt_root: "test/fixtures/prompts"

  model(Spectre.Beam.RuntimeTest.Model)
  use Spectre.Beam

  beaming do
    channel(:sales,
      type: :whatsapp,
      adapter: Spectre.Beam.RuntimeTest.Adapter
    )

    channel(:support,
      type: :whatsapp,
      adapter: Spectre.Beam.RuntimeTest.Adapter,
      planner_exposure: [:send_text]
    )
  end

  protect({:beam, :support, :send_text}, with: :external_message)

  policy :external_message do
    request(:confirm_external)
    accept(:accepted, regex: ~r/^yes$/i)
    reject(:rejected, regex: ~r/^no$/i)
  end

  flow :sales_flow, beam: :sales do
    on :sales_question, regex: ~r/^question$/ do
      reply(:sales_reply)
    end
  end

  flow :support_flow, beam: :support do
    on :support_question, regex: ~r/^question$/ do
      reply(:support_reply)
    end
  end

  flow :generic_flow do
    on :generic_question, regex: ~r/^question$/ do
      reply(:generic_reply)
    end
  end

  flow :notify do
    on :notify_owner, regex: ~r/^notify$/ do
      beam(:owner, via: :support, text: "quote ready")
    end
  end

  flow :notify_sales do
    on :notify_sales, regex: ~r/^notify sales$/ do
      beam("sales-address", via: :sales, text: "sales ready")
    end
  end
end

defmodule Spectre.Beam.RuntimeTest do
  use ExUnit.Case, async: false

  alias Spectre.Action.Provider
  alias Spectre.Beam.RuntimeTest.Agent
  alias Spectre.Invocation
  alias Spectre.Run.Boundary
  alias Spectre.Run.Ref
  alias Spectre.Run.Request

  setup do
    :ok = Spectre.Beam.Store.reset()
    :ok
  end

  test "source-specific flow wins and the reply preserves endpoint affinity" do
    event = %{
      message_id: "sales-1",
      conversation_id: "conversation-1",
      sender: "customer-1",
      text: "question"
    }

    assert {:ok, exchange} =
             Spectre.Beam.handle(
               Agent,
               :sales,
               event,
               adapter_opts: [test_pid: self()]
             )

    assert exchange.turn.result.route.flow == :sales_flow
    assert exchange.input.source.kind == :beam
    assert exchange.input.source.mount == :sales
    assert String.trim(exchange.turn.result.reply_text) == "sales reply"
    assert exchange.receipt.status == :accepted

    assert_receive {:beam_delivered, outbound}
    assert outbound.endpoint == :sales
    assert outbound.conversation_id == "conversation-1"
    assert outbound.to == "customer-1"
    assert outbound.reply_to == "sales-1"
    assert String.trim(outbound.content.text) == "sales reply"
  end

  test "two endpoints of the same type route independently" do
    event = %{
      message_id: "support-1",
      conversation_id: "conversation-2",
      sender: "customer-2",
      text: "question"
    }

    assert {:ok, exchange} =
             Spectre.Beam.handle(
               Agent,
               :support,
               event,
               adapter_opts: [test_pid: self()]
             )

    assert exchange.turn.result.route.flow == :support_flow
    assert String.trim(exchange.turn.result.reply_text) == "support reply"
  end

  test "an inbound retry neither runs a second turn nor delivers twice" do
    event = %{
      message_id: "same-message",
      conversation_id: "conversation-3",
      sender: "customer-3",
      text: "question"
    }

    opts = [adapter_opts: [test_pid: self()]]
    assert {:ok, first} = Spectre.Beam.handle(Agent, :sales, event, opts)
    assert_receive {:beam_delivered, _outbound}

    assert {:ok, duplicate} = Spectre.Beam.handle(Agent, :sales, event, opts)
    assert duplicate.duplicate?

    assert duplicate.turn.result.metadata.runtime_identity.turn_id ==
             first.turn.result.metadata.runtime_identity.turn_id

    refute_receive {:beam_delivered, _outbound}, 50
  end

  test "reactive delivery is fenced by the observable Run reference" do
    event = %{
      message_id: "ref-message",
      conversation_id: "ref-conversation",
      sender: "ref-customer",
      text: "question"
    }

    assert {:ok, inbound} = Spectre.Beam.decode(Agent, :sales, event)

    ref = Ref.new("run-beam-ref", 3, :reply, "boundary-beam-ref")
    result = %Spectre.Result{state: %Spectre.State{}, reply_text: "not the boundary"}

    turn = %Spectre.Turn{
      ref: ref,
      result: result,
      observable: {:reply, "boundary reply", ref}
    }

    opts = [adapter_opts: [test_pid: self()]]

    assert {:ok, first_receipt} = Spectre.Beam.reply(Agent, inbound, turn, opts)
    assert_receive {:beam_delivered, outbound}
    assert outbound.content.text == "boundary reply"
    assert outbound.idempotency_key == "beam-reply:" <> Ref.token(ref)

    assert {:ok, duplicate_receipt} = Spectre.Beam.reply(Agent, inbound, turn, opts)
    assert duplicate_receipt == first_receipt
    refute_receive {:beam_delivered, _outbound}, 50
  end

  test "only an explicitly legacy Turn can use result-based reply fallback" do
    event = %{
      message_id: "legacy-message",
      conversation_id: "legacy-conversation",
      sender: "legacy-customer",
      text: "question"
    }

    assert {:ok, inbound} = Spectre.Beam.decode(Agent, :sales, event)

    result = %Spectre.Result{
      state: %Spectre.State{},
      reply_text: "legacy reply",
      metadata: %{runtime_identity: %{turn_id: "legacy-turn"}}
    }

    legacy_turn = %Spectre.Turn{
      result: result,
      decision: {:reply, result},
      observable: nil
    }

    opts = [adapter_opts: [test_pid: self()]]

    assert {:ok, _receipt} = Spectre.Beam.reply(Agent, inbound, legacy_turn, opts)
    assert_receive {:beam_delivered, outbound}
    assert String.starts_with?(outbound.idempotency_key, "beam-legacy-reply:")

    assert {:error, :beam_turn_boundary_required} =
             Spectre.Beam.reply(Agent, inbound, result, opts)

    refute_receive {:beam_delivered, _outbound}, 50
  end

  test "needs and invocation projections never leak a result reply" do
    event = %{
      message_id: "needs-message",
      conversation_id: "needs-conversation",
      sender: "needs-customer",
      text: "question"
    }

    assert {:ok, inbound} = Spectre.Beam.decode(Agent, :sales, event)

    ref = Ref.new("run-beam-needs", 1, :policy, "boundary-beam-needs")

    boundary = %Boundary{
      id: ref.boundary_id,
      kind: :needs,
      ref: ref,
      request: %Request{id: "request-beam-needs", kind: :policy, name: :confirmation}
    }

    result = %Spectre.Result{state: %Spectre.State{}, reply_text: "must not be delivered"}

    for observable <- [
          {:needs, boundary},
          {:awaiting, %{ref | kind: :invocation}},
          {:reply, nil, %{ref | kind: :complete}}
        ] do
      turn = %Spectre.Turn{ref: ref, result: result, observable: observable}

      assert {:ok, nil} =
               Spectre.Beam.reply(Agent, inbound, turn, adapter_opts: [test_pid: self()])
    end

    refute_receive {:beam_delivered, _outbound}, 50
  end

  test "the inbound pipeline returns policy and invocation boundaries without delivery" do
    opts = [adapter_opts: [test_pid: self()]]

    policy_event = %{
      message_id: "policy-boundary",
      conversation_id: "boundary-conversation",
      sender: "boundary-customer",
      text: "notify"
    }

    assert {:ok, policy_exchange} = Spectre.Beam.handle(Agent, :sales, policy_event, opts)
    assert {:needs, %Boundary{} = policy} = policy_exchange.turn.observable
    assert policy_exchange.turn.boundary == policy
    assert policy_exchange.receipt == nil

    invocation_event = %{
      message_id: "invocation-boundary",
      conversation_id: "boundary-conversation",
      sender: "boundary-customer",
      text: "notify sales"
    }

    assert {:ok, invocation_exchange} =
             Spectre.Beam.handle(Agent, :sales, invocation_event, opts)

    assert {:awaiting, %Ref{} = invocation_ref} = invocation_exchange.turn.observable
    assert %Invocation{ref: ^invocation_ref} = invocation_exchange.turn.boundary
    assert invocation_exchange.receipt == nil

    refute_receive {:beam_delivered, _outbound}, 50
  end

  test "a proactive message is a normal effect and is not auto-executed" do
    assert {:ok, result} = Spectre.ask(Agent, "notify sales")
    assert [%Spectre.Effect{kind: :action, status: :pending} = effect] = result.effects
    assert Spectre.Effect.via(effect) == {:beam, :sales}
    assert effect.name == :send_text
    refute_receive {:beam_delivered, _outbound}

    assert {:ok, completed} =
             Spectre.execute(
               Agent,
               result,
               adapter_opts: [test_pid: self()]
             )

    assert_receive {:beam_delivered, outbound}
    assert outbound.to == "sales-address"
    assert outbound.content.text == "sales ready"

    assert {:ok, %Spectre.Beam.Receipt{status: :accepted}} =
             Spectre.Result.action_outcome(completed)
  end

  test "Spectre policy blocks a protected proactive Beam action" do
    assert {:ok, result} = Spectre.ask(Agent, "notify")
    assert [%Spectre.Effect{status: :waiting_policy}] = result.effects
    assert [%Spectre.Awaitable{status: :open}] = result.awaitables
    refute_receive {:beam_delivered, _outbound}

    assert {:error, {:effect_not_approved, _id}} =
             Spectre.execute(
               Agent,
               result,
               adapter_opts: [test_pid: self()]
             )

    refute_receive {:beam_delivered, _outbound}
  end

  test "planner discovery is opt-in per endpoint and operation" do
    providers = Spectre.ActionConfig.providers(Agent)
    sales = Enum.find(providers, &(&1.id == {:beam, :sales}))
    support = Enum.find(providers, &(&1.id == {:beam, :support}))

    assert {:ok, []} = Provider.actions(sales)
    assert {:ok, support_specs} = Provider.actions(support)
    assert Enum.map(support_specs, & &1.name) == [:send_text]

    assert {:ok, all_specs} = Provider.actions(sales, :all)
    assert Enum.sort(Enum.map(all_specs, & &1.name)) == [:send_document, :send_text]
  end

  test "undeclared Beam endpoints fail while the Agent compiles" do
    module = Module.concat(__MODULE__, "Unknown#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module)} do
      use Spectre.Agent
      use Spectre.Beam

      beaming do
        channel :known, adapter: Spectre.Beam.RuntimeTest.Adapter
      end

      flow :invalid, beam: :missing do
        on :question, regex: ~r/question/ do
          reply "no"
        end
      end
    end
    """

    assert_raise ArgumentError, ~r/unknown_beam_endpoint/, fn ->
      Code.compile_string(source)
    end
  end
end
