defmodule ExKV.AntiEntropy do
  use GenServer

  @interval 30_000  # default sync interval: 30 seconds

  @moduledoc """
  Background anti-entropy worker responsible for ensuring eventual consistency
  across the distributed key-value store.

  Responsibilities:

  - Periodically iterate over known cluster nodes and synchronize keys.
  - Reconcile local and remote metadata to pull missing or newer entries.
  - Push local updates to peers that are behind.
  - Trigger the handoff manager to retry pending writes for nodes that were down.

  Design choices:

  - Uses spawned processes for per-node syncs to avoid blocking the main GenServer.
  - Relies on RPC for remote key lookup and updates.
  """

  ## ------------------------------------------------------------------
  ## Public API / GenServer start
  ## ------------------------------------------------------------------

  # Start the anti-entropy GenServer
  # Registered globally for easy access.
  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(state) do
    # Schedule the first periodic tick immediately
    schedule_tick()
    {:ok, state}
  end

  ## ------------------------------------------------------------------
  ## Internal scheduling
  ## ------------------------------------------------------------------

  # Schedule the next sync tick after @interval milliseconds
  defp schedule_tick, do: Process.send_after(self(), :tick, @interval)

  @impl true
  def handle_info(:tick, state) do
    # Discover cluster nodes from the ring
    ExKV.RingManager.list_nodes()
    |> Enum.each(fn n ->
      # Skip self and only attempt nodes that respond to ping
      if n != node() and Node.ping(n) == :pong do
        spawn(fn -> sync_with(n) end)
      end
    end)

    # Schedule the next periodic tick
    schedule_tick()
    {:noreply, state}
  end

  ## ------------------------------------------------------------------
  ## Synchronization logic
  ## ------------------------------------------------------------------

  # Perform synchronization with a peer node
  # Fetch peer metadata and reconcile differences
  defp sync_with(peer) do
    case :rpc.call(peer, ExKV.AntiEntropy, :metadata_for_peer, [node()], 10_000) do
      {:ok, peer_meta} ->
        local_meta = local_metadata()
        reconcile(peer, local_meta, peer_meta)
        # Also instruct the handoff manager to retry pending hints
        ExKV.HandoffManager.ping_target(peer)
      _ ->
        # Node unreachable or RPC failed, skip for now
        :noop
    end
  end

  # RPC endpoint: return local metadata for a requesting peer
  # Metadata format: %{key => timestamp}
  def metadata_for_peer(_requestor_node) do
    {:ok, local_metadata()}
  end

  # Gather local key metadata (timestamps) from the store
  defp local_metadata do
    case :rpc.call(node(), ExKV.Store, :get_all_meta, [:store], 5_000) do
      {:ok, map} -> map
      _ -> %{}
    end
  end

  ## ------------------------------------------------------------------
  ## Reconciliation between local and peer metadata
  ## ------------------------------------------------------------------

  # Pull missing or outdated entries from peer and push updates the peer lacks
  defp reconcile(peer, local_meta, peer_meta) do
    # Fetch keys from peer that are missing locally or are newer
    Enum.each(peer_meta, fn {key, peer_ts} ->
      local_ts = Map.get(local_meta, key)

      cond do
        local_ts == nil ->
          # Key missing locally, pull from peer
          case :rpc.call(peer, ExKV.Store, :get, [:store, key], 5_000) do
            {val, _ts} -> ExKV.Store.put(:store, key, val)
            _ -> :noop
          end

        local_ts < peer_ts ->
          # Local value outdated, fetch newer value from peer
          case :rpc.call(peer, ExKV.Store, :get, [:store, key], 5_000) do
            {val, _ts} -> ExKV.Store.put(:store, key, val)
            _ -> :noop
          end

        true ->
          # Local value is up-to-date, nothing to do
          :noop
      end
    end)

    # Push local keys that peer is missing or has older versions
    Enum.each(local_meta, fn {key, local_ts} ->
      peer_ts = Map.get(peer_meta, key)

      cond do
        peer_ts == nil ->
          # Peer missing key, push local value
          case ExKV.Store.get(:store, key) do
            {val, _} -> :rpc.call(peer, ExKV.Store, :put, [:store, key, val], 5_000)
            nil -> :noop
          end

        peer_ts < local_ts ->
          # Peer value outdated, push local value
          case ExKV.Store.get(:store, key) do
            {val, _} -> :rpc.call(peer, ExKV.Store, :put, [:store, key, val], 5_000)
            nil -> :noop
          end

        true ->
          # Peer value is up-to-date, skip
          :noop
      end
    end)
  end
end
