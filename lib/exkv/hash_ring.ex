defmodule ExKV.HashRing do
  @moduledoc """
  Consistent hashing ring for distributing keys across nodes.

  - Maintains a sorted list of `{pos, node_id}` tuples as the ring.
  - Uses SHA-256 and extracts 64 bits (unsigned) for positions.
  - Supports virtual nodes (vnode_count per physical node) to smooth key distribution.
  - Provides primary node lookup and replication targets.
  """

  @type node_id :: any()
  @type pos :: integer()

  defstruct ring: [],            # sorted list of {pos, node_id} tuples
            nodes: %{},          # map: node_id => list of positions
            vnode_count: 128     # default number of virtual nodes per physical node

  ## ------------------------------------------------------------------
  ## Public API
  ## ------------------------------------------------------------------

  @doc """
  Create a new, empty ring.

  Options:
  - `:vnode_count` - number of virtual nodes per physical node.
  """
  @spec new(keyword()) :: %__MODULE__{}
  def new(opts \\ []) do
    vnode_count = Keyword.get(opts, :vnode_count, 128)
    %__MODULE__{vnode_count: vnode_count}
  end

  @doc """
  Add a physical node to the ring.

  - Returns an updated ring.
  - No-op if the node already exists.
  """
  @spec add_node(%__MODULE__{}, node_id()) :: %__MODULE__{}
  def add_node(%__MODULE__{} = ring, node_id) do
    add_node(ring, node_id, ring.vnode_count)
  end

  defp add_node(%__MODULE__{} = ring, node_id, vnode_count) do
    if Map.has_key?(ring.nodes, node_id) do
      ring
    else
      # Generate vnode positions deterministically
      positions =
        for i <- 0..(vnode_count - 1) do
          vnode_hash(node_id, i)
        end

      # Merge new positions into the sorted ring
      new_ring_list = (ring.ring ++ Enum.map(positions, &{&1, node_id}))
                      |> Enum.sort_by(fn {pos, _n} -> pos end)

      new_nodes = Map.put(ring.nodes, node_id, positions)

      %__MODULE__{ring | ring: new_ring_list, nodes: new_nodes}
    end
  end

  @doc """
  Remove a physical node from the ring.

  - Returns an updated ring.
  - Removes all virtual nodes for this physical node.
  """
  @spec remove_node(%__MODULE__{}, node_id()) :: %__MODULE__{}
  def remove_node(%__MODULE__{} = ring, node_id) do
    new_ring_list =
      ring.ring
      |> Enum.reject(fn {_pos, n} -> n == node_id end)

    new_nodes = Map.delete(ring.nodes, node_id)

    %__MODULE__{ring | ring: new_ring_list, nodes: new_nodes}
  end

  @doc """
  Find the single node responsible for the given key (primary replica).

  Returns `nil` if the ring is empty.
  """
  @spec find_node(%__MODULE__{}, term()) :: node_id() | nil
  def find_node(%__MODULE__{ring: []}, _key), do: nil

  def find_node(%__MODULE__{ring: ring_list}, key) do
    pos = key_pos(key)

    # Pick first vnode >= pos, wrap around if necessary
    case Enum.find(ring_list, fn {p, _n} -> p >= pos end) do
      nil ->
        # Wrap-around: take the first node
        [{_p, node} | _] = ring_list
        node

      {_p, node} ->
        node
    end
  end

  @doc "Return the raw sorted ring as a list of `{pos, node_id}` tuples."
  @spec to_list(%__MODULE__{}) :: [{pos(), node_id()}]
  def to_list(%__MODULE__{ring: ring}), do: ring

  @doc "Return the list of physical node IDs currently in the ring."
  @spec nodes(%__MODULE__{}) :: [node_id()]
  def nodes(%__MODULE__{nodes: nodes_map}), do: Map.keys(nodes_map)

  ## ------------------------------------------------------------------
  ## Successor / Replication API
  ## ------------------------------------------------------------------

  @doc """
  Return up to `count` distinct successor nodes for the given `key`.

  - Skips virtual-node duplicates.
  - Wraps around the ring if needed.
  - Used to determine replication targets: primary + N-1 replicas.
  """
  @spec successors(%__MODULE__{}, term(), pos_integer()) :: [node_id()]
  def successors(%__MODULE__{ring: []}, _key, _count), do: []

  def successors(%__MODULE__{ring: ring_list}, key, count) when is_integer(count) and count > 0 do
    pos = key_pos(key)

    # Rotate ring so first vnode >= key position
    rotated =
      case Enum.find_index(ring_list, fn {p, _n} -> p >= pos end) do
        nil -> ring_list
        idx -> Enum.drop(ring_list, idx) ++ Enum.take(ring_list, idx)
      end

    collect_successors(rotated, count, [], MapSet.new())
  end

  defp collect_successors(_list, 0, acc, _seen), do: Enum.reverse(acc)
  defp collect_successors([], _count, acc, _seen), do: Enum.reverse(acc)

  defp collect_successors([{_p, node} | rest], count, acc, seen) do
    if MapSet.member?(seen, node) do
      collect_successors(rest, count, acc, seen)
    else
      collect_successors(rest, count - 1, [node | acc], MapSet.put(seen, node))
    end
  end

  @doc """
  Alias for `successors/3` for compatibility with `RingManager` usage.
  """
  @spec find_n_successors(%__MODULE__{}, term(), pos_integer()) :: [node_id()]
  def find_n_successors(%__MODULE__{} = ring, key, n), do: successors(ring, key, n)

  ## ------------------------------------------------------------------
  ## Internal utilities
  ## ------------------------------------------------------------------

  # Convert a key into a 64-bit unsigned integer for the ring
  defp key_pos(key), do: sha64(key)

  # Compute position of a virtual node for a physical node
  defp vnode_hash(node_id, i), do: sha64({node_id, i})

  # SHA-256 -> first 8 bytes -> unsigned 64-bit integer
  defp sha64(term) do
    bin = :erlang.term_to_binary(term)
    digest = :crypto.hash(:sha256, bin)
    <<val::unsigned-big-integer-size(64), _rest::binary>> = digest
    val
  end
end
