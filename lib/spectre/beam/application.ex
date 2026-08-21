defmodule Spectre.Beam.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Spectre.Beam.Registry},
      {Task.Supervisor, name: Spectre.Beam.TaskSupervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: Spectre.Beam.GatewaySupervisor},
      Spectre.Beam.Bus.Local,
      Spectre.Beam.Sequence,
      Spectre.Beam.Store,
      Spectre.Beam.Throttle.Local
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Spectre.Beam.Supervisor
    )
  end
end
