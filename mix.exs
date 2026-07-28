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
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:spectre, github: "elchemista/spectre", ref: "b39b0b1e77d685c0e497cd64d7f16f20d3c1c846"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
