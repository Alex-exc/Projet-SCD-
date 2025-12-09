defmodule ExKV.Config do
  use GenServer
  # Start the config holder process.
  # Here we will do: start a small GenServer that stores runtime configuration.
  # We decided to register it under the module name for easy global lookup.
  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  # Initialize default configuration values.
  # Here we will do: set sensible defaults for replication, quorums and vnodes.
  # We decided to keep `handoff_ttl_ms` in milliseconds for retry/backoff semantics.
  def init(state) do
    state = %{
      replication: 3,
      write_quorum: 2,
      read_quorum: 2,
      vnode_count: 128,
      handoff_ttl_ms: 60_000  # retry duration in ms
    }

    {:ok, state}
  end

  # Public API: get a config value by `key`.
  # Here we will do: provide a synchronous lookup so callers receive immediate config values.
  def get(key), do: GenServer.call(__MODULE__, {:get, key})

  # Handle the sync get request.
  # Here we will use: `Map.get/2` on the stored state and return the result.
  def handle_call({:get, key}, _from, state), do: {:reply, Map.get(state, key), state}
end
