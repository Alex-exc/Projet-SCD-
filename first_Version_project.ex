defmodule ExKV.Store do
  use GenServer

  # --- Module attributes ---
  # Default DETS table name (used internally)
  @default_table_name :kv_store_table

  # --- Public API ---
  # Start the GenServer
  def start_link(opts) do
    # Expect opts to be a keyword list, e.g., [name: MyApp.Store, file: './my_store.dets']
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  # Insert or update a key-value pair
  def put(store_name, key, value) do
    GenServer.call(store_name, {:put, key, value})
  end

  # Retrieve a value by key
  def get(store_name, key) do
    GenServer.call(store_name, {:get, key})
  end

  # Delete a key-value pair
  def delete(store_name, key) do
    GenServer.call(store_name, {:delete, key})
  end

  # --- GenServer Callbacks ---
  
  @impl true
  def init(opts) do
    file = Keyword.fetch!(opts, :file) |> String.to_charlist()

    # Attempt to open the DETS table for persistence
    # If the file doesn't exist, DETS will create it
    case :dets.open_file(@default_table_name, file: file) do
      {:ok, table} ->
        # Load all existing key-value pairs from DETS into a Map
        # This will allow fast in-memory access during runtime
        data_map = :dets.to_list(table) |> Map.new()

        # Initialize the GenServer state with table reference and in-memory data
        initial_state = %{
          table: table,
          data: data_map
        }

        {:ok, initial_state}

      {:error, reason} ->
        # Stop the GenServer if DETS cannot be opened
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    # Write the key-value pair to DETS for persistence
    :ok = :dets.insert(state.table, {key, value})

    # Update the in-memory Map for fast access
    new_state = %{state | data: Map.put(state.data, key, value)}

    # Reply to the caller and update state
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    # Retrieve value from in-memory Map
    value = Map.get(state.data, key)
    {:reply, value, state}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    # Delete key from DETS
    :ok = :dets.delete(state.table, key)

    # Delete key from in-memory Map
    new_state = %{state | data: Map.delete(state.data, key)}

    # Reply to the caller and update state
    {:reply, :ok, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    # Close DETS when the GenServer stops to free resources
    :dets.close(state.table)
    :ok
  end
end
