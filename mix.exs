defmodule Exkv.MixProject do
  use Mix.Project

  def project do
    [
      app: :exkv,
      version: "0.1.0",
      elixir: "~> 1.12",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: []
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ExKV.Application, []}
    ]
  end

  defp deps do
    []
  end

  defp elixirc_paths(_), do: ["lib"]
end
