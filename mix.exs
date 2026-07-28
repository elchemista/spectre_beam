defmodule SpectreBeam.MixProject do
  use Mix.Project

  @version "0.1.2"

  def project do
    [
      app: :spectre_beam,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [plt_add_apps: [:mix]]
    ]
  end

  def application do
    [
      mod: {Spectre.Beam.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      spectre_dep(),
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp spectre_dep do
    case System.get_env("SPECTRE_PATH") do
      path when is_binary(path) and path != "" -> {:spectre, path: Path.expand(path)}
      _other -> {:spectre, github: "elchemista/spectre", branch: "feature/v0.1.2-stack"}
    end
  end
end
