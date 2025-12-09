defmodule ExKV.HandoffManager do
  use GenServer

  @name __MODULE__
  @moduledoc """
  Hinted handoff manager for temporarily unavailable nodes.

  Responsibilities:
  - Buffer writes (hints) that could not reach remote nodes.
  - Retry delivery asynchronously when nodes become reachable.
  - Maintain order and timestamps to prevent lost or stale updates.
  """

  ## ------------------------------------------------------------------
  ## Public API
  ## ------------------------------------------------------------------

  # Start the GenServer.
  # The process is globally registered under the module name for easy access.
  def start_link(_args), do: GenServer.start_link(__MODULE__, %{}, name: @name)

  @doc """
  Store a hint for a target node.

  - `target_node`: the node that was unreachable.
  - `key` and `value`: the key/value to be retried.
  - Hints are stored with timestamp to maintain insertion order.
  """
  def store_hint(target_node, key, value) do
    GenServer.cast(@name, {:store_hint, target_node, key, value})
  end

  @doc """
  Ask the manager to attempt delivery of all buffered hints for a node.

  - This is fire-and-forget; callers do not block.
  - Successfully delivered hints are removed; failures remain for retry.
  """
  def ping_target(target_node) do
    GenServer.cast(@name, {:try_send, target_node})
  end

  ## ------------------------------------------------------------------
  ## GenServer Callbacks
  ## ------------------------------------------------------------------

  @impl true
  def init(state) do
    # Initialize an empty map: node_id => list of {key, value, timestamp}
    # This keeps hints organized per target node for efficient retry.
    {:ok, state}
  end

  @impl true
  def handle_cast({:store_hint, target_node, key, value}, state) do
    # Record a new hint with the current timestamp.
    entry = {key, value, System.system_time(:millisecond)}

    # Prepend to existing list or create a new list for this node
    new_state = Map.update(state, target_node, [entry], fn l -> [entry | l] end)

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:try_send, target_node}, state) do
    # Fetch all buffered hints for the target node
    entries = Map.get(state, target_node, [])

    # Attempt delivery; collect any entries that could not be sent
    {_, remaining} = attempt_send_all(target_node, entries, [])

    # Update state: remove target if all hints sent, otherwise keep remaining
    new_state =
      if remaining == [] do
        Map.delete(state, target_node)
      else
        Map.put(state, target_node, remaining)
      end

    {:noreply, new_state}
  end

  ## ------------------------------------------------------------------
  ## Internal helpers
  ## ------------------------------------------------------------------

  # Attempt to send all entries in order. Preserve remaining entries on failure.
  defp attempt_send_all(_target, [], acc), do: {Enum.reverse(acc), []}

  # Special handling for delete hints marked with `:__delete__`
  defp attempt_send_all(target, [{key, :__delete__, _ts} = entry | rest], acc) do
    case :rpc.call(target, ExKV.Store, :delete, [:store, key], 5_000) do
      :ok ->
        # Successfully deleted remotely; continue with remaining hints
        attempt_send_all(target, rest, acc)

      _failure ->
        # Stop at first failure, preserve this and remaining hints
        {Enum.reverse(acc), [entry | rest]}
    end
  end

  # Normal put hint
  defp attempt_send_all(target, [{key, value, _} = entry | rest], acc) do
    case :rpc.call(target, ExKV.Store, :put, [:store, key, value], 5_000) do
      {:ok, _ts} ->
        # Successfully delivered; continue
        attempt_send_all(target, rest, acc)

      _failure ->
        # Stop on failure; preserve this and remaining hints
        {Enum.reverse(acc), [entry | rest]}
    end
  end
end
