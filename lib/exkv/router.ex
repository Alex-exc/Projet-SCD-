defmodule ExKV.Router do
  @moduledoc false

  alias ExKV.RingManager
  alias ExKV.HandoffManager
  alias ExKV.Config

  # Public API
  # Expose simple `put`/`get`/`delete` entry points that use configured
  # replication and quorum parameters.
  def put(key, value) do
    cfg = Config.get(:replication) || 3
    w = Config.get(:write_quorum) || 2
    replicate_write(key, value, cfg, w)
  end

  def delete(key) do
    cfg = Config.get(:replication) || 3
    w = Config.get(:write_quorum) || 2
    replicate_delete(key, cfg, w)
  end

  def get(key) do
    cfg = Config.get(:replication) || 3
    r = Config.get(:read_quorum) || 2
    read_quorum(key, cfg, r)
  end

  # --- helpers ---

  # Write replication
  # Find the primary + replicas and perform the write on all. We spawn a
  # `Task` per replica and await responses to determine quorum satisfaction.
  defp replicate_write(key, value, replication, write_quorum) do
    nodes = RingManager.find_successors(key, replication)

    tasks =
      Enum.map(nodes, fn node_id ->
        Task.async(fn ->
          if node_id == node() do
            case ExKV.Store.put(:store, key, value) do
              {:ok, ts} -> {:ok, node_id, ts}
              _ -> {:error, node_id}
            end
          else
            case :rpc.call(node_id, ExKV.Store, :put, [:store, key, value], 5_000) do
              {:ok, ts} -> {:ok, node_id, ts}
              _reason ->
                HandoffManager.store_hint(node_id, key, value)
                {:error, node_id}
            end
          end
        end)
      end)

    results = Enum.map(tasks, &Task.await(&1, 6_000))
    oks = Enum.filter(results, fn r -> match?({:ok, _, _}, r) end)

    if length(oks) >= write_quorum do
      {:ok, length(oks)}
    else
      {:error, :write_quorum_not_met}
    end
  end

  # Delete replication
  # Remove the key from primary and replicas and store a hint for remote
  # failures so handoff can retry later.
  defp replicate_delete(key, replication, write_quorum) do
    nodes = RingManager.find_successors(key, replication)

    tasks =
      Enum.map(nodes, fn node_id ->
        Task.async(fn ->
          if node_id == node() do
            :ok = ExKV.Store.delete(:store, key)
            {:ok, node_id}
          else
            case :rpc.call(node_id, ExKV.Store, :delete, [:store, key], 5_000) do
              :ok -> {:ok, node_id}
              _ ->
                HandoffManager.store_hint(node_id, key, :__delete__)
                {:error, node_id}
            end
          end
        end)
      end)

    results = Enum.map(tasks, &Task.await(&1, 6_000))
    oks = Enum.filter(results, fn r -> match?({:ok, _}, r) end)

    if length(oks) >= write_quorum do
      {:ok, length(oks)}
    else
      {:error, :write_quorum_not_met}
    end
  end

  # Read with read-quorum
  # Query the primary then replicas concurrently and collect responses until
  # `read_quorum` valid values are available.
  defp read_quorum(key, replication, read_quorum) do
    nodes = RingManager.find_successors(key, replication)

    tasks =
      Enum.map(nodes, fn node_id ->
        Task.async(fn ->
          if node_id == node() do
            {:ok, ExKV.Store.get(:store, key)}
          else
            case :rpc.call(node_id, ExKV.Store, :get, [:store, key], 5_000) do
              v -> {:ok, v}
              _ -> {:error, node_id}
            end
          end
        end)
      end)

    gather_responses(tasks, read_quorum, [])
  end

  # Gather responses helper: poll tasks, collect valid values, stop when need
  # responses are gathered. We sleep briefly between checks to avoid busy loop.
  defp gather_responses(_tasks, 0, acc), do: choose_latest(acc)

  defp gather_responses(tasks, need, acc) do
    receive do
      after 0 ->
        {finished, running} = Enum.split_with(tasks, &Task.yield(&1, 0))

        finished_results =
          Enum.map(finished, fn t ->
            case Task.yield(t, 0) do
              nil -> {:error, :timeout}
              {:ok, res} -> res
            end
          end)

        vals = Enum.filter(finished_results, fn
          {:ok, nil} -> false
          {:ok, {_, _}} -> true
          {:ok, _} -> true
          _ -> false
        end)

        if length(vals) >= need do
          choose_latest(vals)
        else
          :timer.sleep(50)
          gather_responses(running, need - length(vals), acc ++ vals)
        end
    end
  end

  # Choose the latest value among responses
  defp choose_latest(results) do
    values =
      results
      |> Enum.filter_map(fn
        {:ok, nil} -> false
        {:ok, {value, ts}} -> {value, ts}
        {:ok, v} when is_tuple(v) -> {elem(v, 0), elem(v, 1)}
        _ -> false
      end, fn {v, ts} -> {v, ts} end)

    case values do
      [] -> {:error, :not_found}
      _ ->
        {value, _ts} = Enum.max_by(values, fn {_v, ts} -> ts end)
        {:ok, value}
    end
  end
end
