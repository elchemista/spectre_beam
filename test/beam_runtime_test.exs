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
