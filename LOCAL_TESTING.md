# Testing Locally — Running multiple nodes

This document explains how to run two local Elixir nodes and test the distributed ExKV store.

# LOCAL TESTING — Run two local Elixir nodes and test ExKV

This guide shows how to run two Elixir nodes on one machine (Windows PowerShell), connect them, and perform basic put/get/delete operations against the distributed ExKV store.

Prerequisites
- Elixir and Erlang/OTP installed and available in `PATH`.
- Run commands from the project root (where `mix.exs` is located).
- Use the same Erlang cookie on both nodes so they can communicate.

(1) Start two nodes (open two PowerShell windows)

In Window 1 (node `a`):

```powershell
iex --sname a --cookie secret_cookie -S mix
```

In Window 2 (node `b`):

```powershell
iex --sname b --cookie secret_cookie -S mix
```

Notes:
- Replace `secret_cookie` with a string you choose (both nodes must use the same cookie).
- With `--sname` the full node names will be `a@YOUR_HOSTNAME` and `b@YOUR_HOSTNAME`.

(2) Connect the nodes and register `b` in the ring

From the `a` node (the IEx prompt in Window 1):

```elixir
# Replace YOUR_HOSTNAME with the actual machine hostname shown in each IEx prompt
Node.connect(:"b@YOUR_HOSTNAME")

# Let the ring manager know about node b so it participates in the ring
ExKV.RingManager.add_node(:"b@YOUR_HOSTNAME")
```

You can verify the connection:

```elixir
Node.list()
ExKV.RingManager.list_nodes()
```

(3) Put/Get/Delete examples

# On any node (a or b) you can store and retrieve keys. Examples below run on node `a`:

```elixir
# Put a value (value can be any Elixir term)
ExKV.put("my_key", "hello world")

# Get the value back
ExKV.get("my_key")

# Delete the key
ExKV.delete("my_key")
```

(4) Quick checks and debugging

- Inspect ring members and successors:

```elixir
ExKV.RingManager.list_nodes()
ExKV.RingManager.find_successors("my_key", 3)
```

- Check local store keys and metadata (helpful for anti-entropy / rebalancer tests):

```elixir
ExKV.Store.get_all_keys()
ExKV.Store.get_all_meta()
```

(5) Notes and next steps

- If you add/remove nodes you may need to call `ExKV.RingManager.add_node/1` or `remove_node/1` on a reachable manager process so the topology is updated.
- The project includes an HTTP router at `exkv/http/router.ex` for simple external testing — you can start the app and send HTTP requests if `Plug.Cowboy` is configured.
- For deeper testing, run one node in debug mode and watch the `handoff_manager` and `anti_entropy` logs to observe replication and hinted handoff behavior.

If you want, I can run `mix compile` and a quick smoke test now (or walk through any failing runtime errors you see). 