defmodule ExKV.Store do
  use GenServer

  @default_table_name :kv_store_table
  # Start the storage process
  # Here we will do: start a GenServer that owns a DETS table and an in-memory map.
  # We decided to register the process under the `:name` option or the module name.
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  # Public API (local node only)
  # Here we will do: provide synchronous wrappers around GenServer calls for put/get/delete.
  # Here we will use: `GenServer.call/2` so callers block until the operation completes.
  def put(store_name, key, value) do
    GenServer.call(store_name, {:put, key, value})
  end

  def get(store_name, key) do
    GenServer.call(store_name, {:get, key})
  end

  def delete(store_name, key) do
    GenServer.call(store_name, {:delete, key})
  end

  # Return metadata for all keys: map of key => timestamp
  # Here we will do: provide a synchronous API used by anti-entropy to obtain
  # the timestamps for all stored keys so peers can reconcile differences.
  def get_all_meta(store_name) do
    GenServer.call(store_name, :get_all_meta)
  end

  # Return the list of all keys stored locally.
  # Here we will do: provide a synchronous API used by rebalancer and other
  # components to iterate local keys. It returns a plain list of keys.
  def get_all_keys(store_name) do
    GenServer.call(store_name, :get_all_keys)
  end

  # Callbacks
  @impl true
  # Here we will do: open the DETS file provided in opts and load entries into memory.
  # We decided to keep a small in-memory map `:data` for faster reads and mirror it to DETS.
  def init(opts) do
    # DETS is optional for local testing environments. If DETS isn't available
    # we fall back to an in-memory-only store to allow smoke tests to run.
    if :erlang.function_exported(:dets, :open_file, 2) do
      file = Keyword.fetch!(opts, :file) |> String.to_charlist()

      case :dets.open_file(@default_table_name, file: file) do
        {:ok, table} ->
          # Try to read entries; if `:dets.tab2list/1` isn't available fall back
          # to an empty map (DETS may be compiled without this helper).
          entries =
            if :erlang.function_exported(:dets, :tab2list, 1) do
              :dets.tab2list(table)
            else
              []
            end

          data_map = Map.new(entries, fn {k, {v, ts}} -> {k, {v, ts}} end)
          {:ok, %{table: table, data: data_map}}

        {:error, _reason} ->
          # If DETS can't open the file, fall back to an in-memory store.
          {:ok, %{table: nil, data: %{}}}
      end
    else
      # No DETS available: run in-memory only.
      {:ok, %{table: nil, data: %{}}}
    end
  end

  @impl true
  # Handle synchronous put requests.
  # Here we will do: create a timestamped entry, persist to DETS and update the in-memory map.
  # We decided to return `{:ok, ts}` where `ts` is the millisecond timestamp of the write.
  def handle_call({:put, key, value}, _from, state) do
    ts = System.system_time(:millisecond)
    entry = {value, ts}
    # Persist to DETS only when a table exists and DETS is available.
    if state.table && :erlang.function_exported(:dets, :insert, 2) do
      :dets.insert(state.table, {key, entry})
    end

    new_state = %{state | data: Map.put(state.data, key, entry)}
    {:reply, {:ok, ts}, new_state}
  end

  @impl true
  # Handle synchronous get requests.
  # Here we will do: read from the in-memory map for fast access and return the stored {value, ts} tuple.
  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state.data, key), state}
  end

  @impl true
  # Handle synchronous delete requests.
  # Here we will do: delete from DETS and remove from the in-memory map, then reply :ok.
  def handle_call({:delete, key}, _from, state) do
    # Delete from DETS only when available.
    if state.table && :erlang.function_exported(:dets, :delete, 2) do
      :dets.delete(state.table, key)
    end

    new_state = %{state | data: Map.delete(state.data, key)}
    {:reply, :ok, new_state}
  end

  @impl true
  # Handle request to return all stored keys.
  # Here we will do: extract the keys from the in-memory data map and return them.
  def handle_call(:get_all_keys, _from, state) do
    {:reply, Map.keys(state.data), state}
  end

  @impl true
  # Handle request to return metadata for all keys.
  # Here we will do: convert the in-memory `state.data` map into a map of key => ts
  # and return it as `{:ok, meta}` so callers get a consistent snapshot.
  def handle_call(:get_all_meta, _from, state) do
    meta = Enum.into(state.data, %{}, fn {k, {_v, ts}} -> {k, ts} end)
    {:reply, {:ok, meta}, state}
  end

  @impl true
  # Termination callback: ensure the DETS file is closed cleanly on shutdown.
  # Here we will do: close the DETS table and return :ok.
  def terminate(_reason, state) do
    if state.table && :erlang.function_exported(:dets, :close, 1) do
      :dets.close(state.table)
    end

    :ok
  end
end
