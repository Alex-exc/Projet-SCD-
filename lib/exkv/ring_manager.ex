defmodule ExKV.RingManager do
  @moduledoc """
  Single, registered process that owns the consistent-hash ring.

  Responsibilities
  - Maintain a local view of the cluster ring (using `ExKV.HashRing`).
  - Serialize updates to the ring (joins/leaves).
  - Provide lookups for key ownership and replica targets.
  - Trigger asynchronous rebalancing when the ring changes.

  Notes
  - The process is registered under `ExKV.RingManager` so other modules can
    call it directly.
  - `vnode_count` is configurable at startup via `init/1` opts; default is 128.
  """

  use GenServer
  alias ExKV.HashRing

  ## ------------------------------------------------------------------
  ## Public API
  ## ------------------------------------------------------------------

  @doc """
  Start and register the ring manager.

  Options
  - `:name` (unused by default — the process registers as the module name)
  - `:vnode_count` — optional integer to set virtual node count for better distribution.

  The process is registered as `ExKV.RingManager`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    # Intentionally register under the module name for global lookup.
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Add a physical node to the ring. Synchronous — returns `:ok` when applied.`"
  @spec add_node(node()) :: :ok
  def add_node(node_id),
    do: GenServer.call(__MODULE__, {:add_node, node_id})

  @doc "Remove a physical node from the ring. Synchronous — returns `:ok` when applied.`"
  @spec remove_node(node()) :: :ok
  def remove_node(node_id),
    do: GenServer.call(__MODULE__, {:remove_node, node_id})

  @doc """
  Return the single node responsible for `key` (primary owner).
  The returned value is a physical node identifier (the same type used in add_node/1).
  """
  @spec find_node(term()) :: node()
  def find_node(key),
    do: GenServer.call(__MODULE__, {:find_node, key})

  @doc """
  Return the first `n` distinct nodes responsible for `key`, suitable for replication.
  The order is primary first, then successors.
  """
  @spec find_successors(term(), non_neg_integer()) :: [node()]
  def find_successors(key, n),
    do: GenServer.call(__MODULE__, {:find_successors, key, n})

  @doc "Return the set/list of all physical nodes currently present in the ring.`"
  @spec list_nodes() :: [node()]
  def list_nodes(),
    do: GenServer.call(__MODULE__, :list_nodes)

  ## ------------------------------------------------------------------
  ## GenServer Callbacks
  ## ------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Allow configurable vnode count for testing & tuning. Using many vnodes
    # smooths key distribution but increases ring memory usage.
    vnode_count = Keyword.get(opts, :vnode_count, 128)

    # Initialize a fresh, empty ring.
    ring = HashRing.new(vnode_count: vnode_count)

    # Register the local BEAM node so it participates in ownership immediately.
    # Using `node()` is conventional: it returns the local node name.
    ring = HashRing.add_node(ring, node())

    {:ok, ring}
  end

  @impl true
  def handle_call({:add_node, node_id}, _from, ring) do
    # Create an updated ring with the new node.
    new_ring = HashRing.add_node(ring, node_id)

    # Kick off rebalancing asynchronously. We use Task.start/1 here instead of
    # bare `spawn/1` to make intent clearer and to allow future instrumentation.
    # The rebalancer is expected to be resilient to concurrent runs.
    Task.start(fn ->
      # Rebalancer must handle failures internally (shouldn't crash supervisor).
      ExKV.Rebalancer.rebalance(node_id, new_ring)
    end)

    {:reply, :ok, new_ring}
  end

  @impl true
  def handle_call({:remove_node, node_id}, _from, ring) do
    # Compute the ring after removal.
    new_ring = HashRing.remove_node(ring, node_id)

    # Asynchronously handle redistribution and cleanup for the removed node.
    Task.start(fn ->
      ExKV.Rebalancer.handle_node_removal(node_id, new_ring)
    end)

    {:reply, :ok, new_ring}
  end

  @impl true
  def handle_call({:find_node, key}, _from, ring) do
    # Pure lookup; does not modify state. Return `nil` if no nodes exist.
    {:reply, HashRing.find_node(ring, key), ring}
  end

  @impl true
  def handle_call({:find_successors, key, n}, _from, ring) do
    # Delegate to HashRing logic which should return distinct nodes.
    successors = HashRing.find_n_successors(ring, key, n)
    {:reply, successors, ring}
  end

  @impl true
  def handle_call(:list_nodes, _from, ring) do
    {:reply, HashRing.nodes(ring), ring}
  end

  ## ------------------------------------------------------------------
  ## Notes & Warnings
  ## ------------------------------------------------------------------
  # - All public functions use synchronous `GenServer.call/2`. This ensures
  #   callers observe the ring state immediately after their operation; for
  #   very hot update paths you might prefer `cast` or batched updates.
  # - Rebalancing is triggered asynchronously; the `RingManager` does not
  #   block writes or membership changes while rebalancing runs.
end
