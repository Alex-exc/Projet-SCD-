defmodule ExKV.Application do
  use Application

  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT", "4000"))

    children = [
      {Finch, name: ExKV.Finch},
      {ExKV.Store, file: "exkv_store_#{port}.dets"},
      {Plug.Cowboy, scheme: :http, plug: ExKV.Router, options: [port: port]}
    ]

    opts = [strategy: :one_for_one, name: ExKV.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
