defmodule Spectre.Beam.InstanceIdentityContractTest.Adapter do
  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Content
  alias Spectre.Beam.Inbound
  alias Spectre.Beam.Receipt

  @impl true
  def capabilities(_opts), do: MapSet.new([:text])

  @impl true
  def decode(event, _opts) do
    {:ok,
     %Inbound{
       message_id: event.message_id,
       conversation_id: event.conversation_id,
       sender: event.sender,
       recipient: "agent",
       content: Content.text(event.text),
       authenticated?: Map.get(event, :authenticated?, false),
       occurred_at: ~U[2026-07-30 10:00:00Z],
       metadata: %{}
     }}
  end

  @impl true
  def deliver(outbound, _opts), do: {:ok, Receipt.accepted(outbound)}
end

defmodule Spectre.Beam.InstanceIdentityContractTest.Agent do
  use Spectre.Agent
  use Spectre.Beam

  beaming do
    channel(:telegram,
      type: :telegram,
      adapter: Spectre.Beam.InstanceIdentityContractTest.Adapter
    )

    channel(:whatsapp,
      type: :whatsapp,
      adapter: Spectre.Beam.InstanceIdentityContractTest.Adapter
    )

    channel(:telegram_builtin,
      type: :telegram,
      adapter: Spectre.Beam.Adapters.ExGram
    )
  end

  flow :linked_reply, beam: [:telegram, :whatsapp] do
    on :hello, regex: ~r/^hello$/ do
      run(:linked_reply)
    end
  end

  def linked_reply(_input, _context), do: "linked"
end

defmodule Spectre.Beam.InstanceIdentityContractTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.InstanceIdentityContractTest.Agent
  alias Spectre.Beam.Store
  alias Spectre.Instance
  alias Spectre.Subject
  alias Spectre.Subject.Registry, as: SubjectRegistry

  @subject_registry __MODULE__.SubjectRegistry
  @instance_supervisor __MODULE__.InstanceSupervisor

  setup do
    :ok = Store.reset()

    start_supervised!({SubjectRegistry, name: @subject_registry})
    start_supervised!({Spectre.Supervisor, name: @instance_supervisor})

    :ok
  end

  test "two explicitly linked channel identities resolve to one Subject Instance" do
    subject = Subject.new("account-42")
    telegram = inbound(:telegram, "telegram-user", "telegram-chat")
    whatsapp = inbound(:whatsapp, "whatsapp-user", "whatsapp-chat")

    assert {:ok, telegram_identity} =
             Spectre.Beam.external_identity(telegram,
               authenticated_at: 1,
               proof_ref: "telegram-signature"
             )

    assert {:ok, whatsapp_identity} =
             Spectre.Beam.external_identity(whatsapp,
               authenticated_at: 2,
               proof_ref: "whatsapp-signature"
             )

    assert {:ok, _telegram_link} =
             SubjectRegistry.bind(
               @subject_registry,
               Agent,
               subject,
               telegram_identity,
               proof: "telegram-bootstrap"
             )

    assert {:ok, _whatsapp_link} =
             SubjectRegistry.bind(
               @subject_registry,
               Agent,
               subject,
               whatsapp_identity,
               proof: "whatsapp-bootstrap"
             )

    assert {:ok, telegram_instance} =
             Spectre.Beam.resolve_instance(
               @instance_supervisor,
               Agent,
               telegram,
               identity_opts()
             )

    assert {:ok, whatsapp_instance} =
             Spectre.Beam.resolve_instance(
               @instance_supervisor,
               Agent,
               whatsapp,
               identity_opts()
             )

    assert telegram_instance == whatsapp_instance
    assert Instance.ref(telegram_instance).subject == subject
  end

  test "handle_instance reaches the normal Turn boundary through the linked Instance" do
    event = event(:telegram, "linked-user", "linked-chat", "message-1")
    assert {:ok, decoded} = Spectre.Beam.decode(Agent, :telegram, event)
    assert {:ok, identity} = Spectre.Beam.external_identity(decoded, authenticated_at: 10)

    assert {:ok, _link} =
             SubjectRegistry.bind(
               @subject_registry,
               Agent,
               Subject.new("linked-account"),
               identity,
               proof: "verified-bootstrap"
             )

    assert {:ok, exchange} =
             Spectre.Beam.handle_instance(
               @instance_supervisor,
               Agent,
               :telegram,
               event,
               identity_opts()
             )

    assert {:reply, "linked", _ref} = exchange.turn.observable
    assert exchange.receipt.status == :accepted
  end

  test "same sender and conversation never create continuity without an explicit link" do
    telegram = inbound(:telegram, "same-user", "same-conversation")
    whatsapp = inbound(:whatsapp, "same-user", "same-conversation")
    subject = Subject.new("explicit-subject")

    assert {:ok, telegram_identity} =
             Spectre.Beam.external_identity(telegram, authenticated_at: 20)

    assert {:ok, _link} =
             SubjectRegistry.bind(
               @subject_registry,
               Agent,
               subject,
               telegram_identity,
               proof: "telegram-proof"
             )

    assert {:ok, _instance} =
             Spectre.Beam.resolve_instance(
               @instance_supervisor,
               Agent,
               telegram,
               identity_opts()
             )

    assert {:error, :unlinked_external_identity} =
             Spectre.Beam.resolve_instance(
               @instance_supervisor,
               Agent,
               whatsapp,
               identity_opts()
             )
  end

  test "unauthenticated inbound fails before Subject resolution" do
    inbound = %{inbound(:telegram, "user", "chat") | authenticated?: false}

    assert {:error, :beam_external_identity_not_authenticated} =
             Spectre.Beam.external_identity(inbound)

    assert {:error, :beam_external_identity_not_authenticated} =
             Spectre.Beam.resolve_instance(
               @instance_supervisor,
               Agent,
               inbound,
               identity_opts()
             )
  end

  test "external identity principals must be portable logical values" do
    inbound = inbound(:telegram, self(), "chat")
    blank = inbound(:telegram, "   ", "chat")
    missing = %{inbound | sender: nil}

    assert {:error,
            {:invalid_beam_external_identity_sender, {:nonportable_run_value, _path, :pid}}} =
             Spectre.Beam.external_identity(inbound)

    assert {:error, :beam_external_identity_sender_required} =
             Spectre.Beam.external_identity(blank)

    assert {:error, :beam_external_identity_sender_required} =
             Spectre.Beam.external_identity(missing)
  end

  test "identity options fail closed before touching Spectre registries" do
    inbound = inbound(:telegram, "user", "chat")

    assert {:error, {:invalid_beam_identity_options, %{}}} =
             Spectre.Beam.external_identity(inbound, %{})

    assert {:error, {:invalid_beam_identity_options, [:not_a_keyword]}} =
             Spectre.Beam.external_identity(inbound, [:not_a_keyword])

    assert {:error, {:invalid_beam_authentication_time, :invalid}} =
             Spectre.Beam.external_identity(inbound, authenticated_at: :invalid)

    assert {:error, {:invalid_beam_identity_metadata, []}} =
             Spectre.Beam.external_identity(inbound, identity_metadata: [])

    assert {:error, {:invalid_beam_instance_options, %{}}} =
             Spectre.Beam.resolve_instance(
               @instance_supervisor,
               Agent,
               inbound,
               Keyword.put(identity_opts(), :instance_opts, %{})
             )

    assert {:error, {:invalid_beam_identity_options, %{}}} =
             Spectre.Beam.resolve_instance(@instance_supervisor, Agent, inbound, %{})

    assert {:error, {:invalid_beam_identity_inbound, :invalid}} =
             Spectre.Beam.external_identity(:invalid)

    assert {:error, {:invalid_beam_identity_inbound, :invalid}} =
             Spectre.Beam.resolve_instance(@instance_supervisor, Agent, :invalid)

    assert {:error, {:invalid_beam_instance_options, [:not_a_keyword]}} =
             Spectre.Beam.resolve_instance(
               @instance_supervisor,
               Agent,
               inbound,
               Keyword.put(identity_opts(), :instance_opts, [:not_a_keyword])
             )

    assert {:error, {:invalid_beam_agent_ref, 12}} =
             Spectre.Beam.resolve_instance(@instance_supervisor, 12, inbound, identity_opts())

    at = ~U[2026-08-21 00:00:00Z]
    assert {:ok, identity} = Spectre.Beam.external_identity(inbound, authenticated_at: at)
    assert identity.authenticated_at == DateTime.to_unix(at, :millisecond)
  end

  test "an unverified event cannot impersonate an explicitly linked sender" do
    authenticated_event = event(:telegram, "linked-user", "linked-chat", "authenticated-message")
    assert {:ok, authenticated} = Spectre.Beam.decode(Agent, :telegram, authenticated_event)
    assert {:ok, identity} = Spectre.Beam.external_identity(authenticated, authenticated_at: 30)

    assert {:ok, _link} =
             SubjectRegistry.bind(
               @subject_registry,
               Agent,
               Subject.new("protected-account"),
               identity,
               proof: "trusted-bootstrap"
             )

    spoofed_event =
      authenticated_event
      |> Map.delete(:authenticated?)
      |> Map.put(:message_id, "spoofed-message")

    assert {:error, :beam_external_identity_not_authenticated} =
             Spectre.Beam.handle_instance(
               @instance_supervisor,
               Agent,
               :telegram,
               spoofed_event,
               identity_opts()
             )
  end

  test "built-in adapters fail closed until the host marks provider authentication verified" do
    raw_event = %{
      id: 1,
      chat_id: 10,
      sender_id: 20,
      from_me: false,
      text: "hello"
    }

    assert {:ok, inbound} = Spectre.Beam.decode(Agent, :telegram_builtin, raw_event)
    refute inbound.authenticated?

    assert {:error, :beam_external_identity_not_authenticated} =
             Spectre.Beam.handle_instance(
               @instance_supervisor,
               Agent,
               :telegram_builtin,
               raw_event,
               identity_opts()
             )
  end

  test "inbound deduplication is isolated by Agent Instance" do
    first_event = event(:telegram, "first-user", "first-chat", "provider-shared-id")
    second_event = event(:telegram, "second-user", "second-chat", "provider-shared-id")

    assert {:ok, first_inbound} = Spectre.Beam.decode(Agent, :telegram, first_event)
    assert {:ok, second_inbound} = Spectre.Beam.decode(Agent, :telegram, second_event)

    assert {:ok, first_identity} =
             Spectre.Beam.external_identity(first_inbound, authenticated_at: 40)

    assert {:ok, second_identity} =
             Spectre.Beam.external_identity(second_inbound, authenticated_at: 41)

    assert {:ok, _first_link} =
             SubjectRegistry.bind(
               @subject_registry,
               Agent,
               Subject.new("first-account"),
               first_identity,
               proof: "first-proof"
             )

    assert {:ok, _second_link} =
             SubjectRegistry.bind(
               @subject_registry,
               Agent,
               Subject.new("second-account"),
               second_identity,
               proof: "second-proof"
             )

    assert {:ok, first_exchange} =
             Spectre.Beam.handle_instance(
               @instance_supervisor,
               Agent,
               :telegram,
               first_event,
               identity_opts()
             )

    assert {:ok, second_exchange} =
             Spectre.Beam.handle_instance(
               @instance_supervisor,
               Agent,
               :telegram,
               second_event,
               identity_opts()
             )

    assert first_exchange.inbound.sender == "first-user"
    assert second_exchange.inbound.sender == "second-user"
    refute first_exchange.duplicate?
    refute second_exchange.duplicate?
  end

  test "inbound deduplication is isolated by conversation within one Instance" do
    first_event = event(:telegram, "shared-user", "direct-chat", "chat-scoped-id")
    second_event = event(:telegram, "shared-user", "group-chat", "chat-scoped-id")

    assert {:ok, inbound} = Spectre.Beam.decode(Agent, :telegram, first_event)
    assert {:ok, identity} = Spectre.Beam.external_identity(inbound, authenticated_at: 50)

    assert {:ok, _link} =
             SubjectRegistry.bind(
               @subject_registry,
               Agent,
               Subject.new("shared-account"),
               identity,
               proof: "shared-proof"
             )

    assert {:ok, first_exchange} =
             Spectre.Beam.handle_instance(
               @instance_supervisor,
               Agent,
               :telegram,
               first_event,
               identity_opts()
             )

    assert {:ok, second_exchange} =
             Spectre.Beam.handle_instance(
               @instance_supervisor,
               Agent,
               :telegram,
               second_event,
               identity_opts()
             )

    assert first_exchange.inbound.conversation_id == "direct-chat"
    assert second_exchange.inbound.conversation_id == "group-chat"
    refute first_exchange.duplicate?
    refute second_exchange.duplicate?
  end

  defp inbound(endpoint, sender, conversation_id) do
    event = event(endpoint, sender, conversation_id, "decoded-#{endpoint}")
    {:ok, inbound} = Spectre.Beam.decode(Agent, endpoint, event)
    inbound
  end

  defp event(_endpoint, sender, conversation_id, message_id) do
    %{
      message_id: message_id,
      conversation_id: conversation_id,
      sender: sender,
      authenticated?: true,
      text: "hello"
    }
  end

  defp identity_opts do
    [
      authenticated_at: 100,
      subject_registry: @subject_registry
    ]
  end
end
