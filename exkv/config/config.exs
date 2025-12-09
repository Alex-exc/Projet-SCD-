import Config

# Disable code reloading and watch mode for production / docker
config :exkv, ExKV.Endpoint,
  code_reloader: false,
  check_origin: false

config :phoenix, :plug_init_mode, :runtime

