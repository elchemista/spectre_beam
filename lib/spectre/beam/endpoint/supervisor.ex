defmodule Spectre.Beam.Endpoint.Supervisor do
  @moduledoc """
  Supervises the two processes that make one channel live.

  `Spectre.Beam.Endpoint.Server` owns the provider client and the ingress;
  `Spectre.Beam.Outbox` drains deliveries against it. The strategy is
  `:rest_for_one` because an outbox holding a client that has just been
  re-resolved would deliver through a stale connection.
  """

  use Supervisor

  alias Spectre.Beam.Endpoint.Server, as: EndpointServer
  alias Spectre.Beam.Gateway.Spec
  alias Spectre.Beam.Outbox

  @registry Spectre.Beam.Registry

  @doc false
  @spec child_spec({Spec.t(), term()}) :: Supervisor.child_spec()
  def child_spec({%Spec{} = spec, id}) do
    %{
      id: {__MODULE__, spec.name, id},
      start: {__MODULE__, :start_link, [{spec, id}]},
      type: :supervisor
    }
  end

  @spec start_link({Spec.t(), term()}) :: Supervisor.on_start()
  def start_link({%Spec{} = spec, id}) do
    Supervisor.start_link(__MODULE__, {spec, id},
      name: {:via, Registry, {@registry, {:endpoint_supervisor, spec.name, id}}}
    )
  end

  @impl Supervisor
  def init({spec, id}) do
    children = [
      {EndpointServer, {spec, id}},
      {Outbox, {spec, id}}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
