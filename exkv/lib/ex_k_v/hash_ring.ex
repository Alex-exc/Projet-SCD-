defmodule ExKV.HashRing do
  @moduledoc """
  Simple consistent hashing ring.
  """

  def hash(value) do
    :erlang.phash2(value)
  end

  def build(nodes) do
    nodes
    |> Enum.map(&{hash(&1), &1})
    |> Enum.sort()
  end

  def find_node(key, ring) do
    key_hash = hash(key)

    case Enum.find(ring, fn {h, _node} -> h >= key_hash end) do
      nil ->
        {_, node} = List.first(ring)
        node

      {_, node} ->
        node
    end
  end

  def replication_nodes(key, ring, n) do
    primary = find_node(key, ring)
    idx = Enum.find_index(ring, fn {_, node} -> node == primary end)

    Enum.map(0..(n - 1), fn offset ->
      {_, node} = Enum.at(ring, rem(idx + offset, length(ring)))
      node
    end)
  end
end
