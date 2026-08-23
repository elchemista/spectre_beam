defmodule SpectreBeam.MixProject do
  use Mix.Project

  @version "0.1.0"
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
      spectre_dep(),
      # Optional: enables JSON framing on the local control socket. Without it
      # the socket falls back to ETF, which only Elixir clients can read.
      {:jason, "~> 1.4", optional: true},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp spectre_dep do
    case System.get_env("SPECTRE_PATH") do
      path when is_binary(path) and path != "" ->
        {:spectre, path: Path.expand(path, __DIR__), only: :test, override: true}

      _unset ->
        {:spectre, "~> 0.3.3", only: :test}
    end
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: ["README.md", "docs/PUBLIC_API.md", "CHANGELOG.md", "LICENSE"]
    ]
  end
end
