defmodule SpectreBeam.MixProject do
  use Mix.Project

  @version "0.1.6"
  @source_url "https://github.com/elchemista/spectre_beam"

  def project do
    [
      app: :spectre_beam,
      name: "Spectre Beam",
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "External-channel adapters and delivery boundary for Spectre agents.",
      package: package(),
      dialyzer: [plt_add_apps: [:mix]],
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url
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
      {:spectre, github: "elchemista/spectre", branch: "main", only: :test},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["elchemista"],
      files: ~w(lib docs mix.exs README.md CHANGELOG.md LICENSE),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "main",
      extras: ["README.md", "docs/PUBLIC_API.md", "CHANGELOG.md", "LICENSE"]
    ]
  end
end
