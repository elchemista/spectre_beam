defmodule Spectre.Beam.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(
      [Spectre.Beam.Store, Spectre.Beam.Throttle.Local],
      strategy: :one_for_one,
      name: Spectre.Beam.Supervisor
    )
  end
end
