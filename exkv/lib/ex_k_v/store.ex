defmodule ExKV.Store do
  use GenServer

  @table_name :kv_store_table

  # -------------------------
  # Public API for Router
  # -------------------------

  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  def put(key, value) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  def delete(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  # -------------------------
  # Public API for multi-node
  # -------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get(server, key) do
    GenServer.call(server, {:get, key})
  end

  def put(server, key, value) do
    GenServer.call(server, {:put, key, value})
  end

  def delete(server, key) do
    GenServer.call(server, {:delete, key})
  end

  # -------------------------
  # GenServer Callbacks
  # -------------------------

  @impl true
  def init(opts) do
    file =
      opts
      |> Keyword.fetch!(:file)
      |> String.to_charlist()

    case :dets.open_file(@table_name, file: file) do
      {:ok, table} ->
        data =
          :dets.foldl(
            fn {key, value}, acc ->
              Map.put(acc, key, value)
            end,
            %{},
            table
          )

        {:ok, %{table: table, data: data}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    :ok = :dets.insert(state.table, {key, value})
    new_state = %{state | data: Map.put(state.data, key, value)}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state.data, key), state}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    :ok = :dets.delete(state.table, key)
    new_state = %{state | data: Map.delete(state.data, key)}
    {:reply, :ok, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
    :ok
  end
end
