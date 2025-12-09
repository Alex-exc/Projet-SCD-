defmodule ExKV.Rebalancer do
  @moduledoc """
  Handles key redistribution when nodes join or leave the cluster.

  Responsibilities:
  - On node join: transfer ownership of keys now assigned to the new node.
  - On node removal: redistribute keys or fetch from replicas.
  - Ensure minimal disruption for ongoing reads/writes.
  """

  @doc """
  Rebalance keys to a newly joined node.

  - `new_node`: the node that has just joined the ring.
  - `ring`: the updated HashRing after the node addition.

  Algorithm:
  1. Iterate all local keys.
  2. For each key, check if `new_node` is now responsible.
  3. If yes and `new_node` is not the current node, send the key/value via RPC.
  """
  def rebalance(new_node, _ring) do
    # Fetch all local keys. This will only include keys stored on this node.
    keys = ExKV.Store.get_all_keys(:store)

    Enum.each(keys, fn key ->
      responsible = ExKV.RingManager.find_node(key)

      # Only move keys for which the new node is now primary, and avoid self-transfer.
      if responsible == new_node and responsible != node() do
        case ExKV.Store.get(:store, key) do
          {val, _ts} ->
            # Attempt an RPC call to store the key on the new node
            # Timeout of 5 seconds; in production, consider retry/backoff logic
            try do
              :rpc.call(new_node, ExKV.Store, :put, [:store, key, val], 5_000)
            catch
              _kind, reason ->
                IO.warn("Failed to send key #{inspect(key)} to #{inspect(new_node)}: #{inspect(reason)}")
            end

          _ ->
            # Key not found locally; could have been deleted concurrently
            :noop
        end
      end
    end)
  end

  @doc """
  Handle redistribution when a node leaves the cluster.

  - `removed_node`: the node that has left.
  - `ring`: the updated ring after removal.

  Current implementation is a placeholder:
  - Ideally, fetch keys that were owned by the removed node from replicas.
  - Populate local store if necessary.
  - Ensure eventual consistency via anti-entropy.
  """
  def handle_node_removal(_removed_node, _ring) do
    # TODO: implement actual key recovery from replicas or hinted-handoff
    :ok
  end
end
