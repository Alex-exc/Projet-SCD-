defmodule ExKV.NodeRegistry do
  @moduledoc """
  Static list of nodes inside Docker Compose.
  """

  def nodes do
    [
      "http://exkv_node1:4000",
      "http://exkv_node2:4000",
      "http://exkv_node3:4000"
    ]
  end
end
