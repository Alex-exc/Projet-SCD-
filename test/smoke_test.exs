host = System.get_env("COMPUTERNAME") || "localhost"
a = String.to_atom("a@" <> host)
b = String.to_atom("b@" <> host)

IO.puts("Test running on host: #{host}")
IO.puts("Connecting to nodes: #{inspect(a)}, #{inspect(b)}")

# Ensure this process is a node so we can perform RPCs
Node.start(String.to_atom("tmp@" <> host))
Node.set_cookie(String.to_atom(System.get_env("ERLANG_COOKIE") || System.get_env("COOKIE") || "secret_cookie"))

Node.connect(a)
Node.connect(b)

IO.puts("Adding node #{inspect(b)} to ring on #{inspect(a)}")
:rpc.call(a, ExKV.RingManager, :add_node, [b])

IO.puts("Putting k1 -> \"hello\" via node #{inspect(a)}")
:rpc.call(a, ExKV, :put, ["k1", "hello"])

IO.puts("Reading from b before stopping primary:")
res1 = :rpc.call(b, ExKV, :get, ["k1"]) 
IO.inspect(res1)

primary = :rpc.call(a, ExKV.RingManager, :find_node, ["k1"])
IO.puts("Primary according to a: #{inspect(primary)}")

IO.puts("Stopping primary node: #{inspect(primary)}")
:rpc.call(primary, :init, :stop, [])
:timer.sleep(500)

IO.puts("Reading from b after stopping primary:")
res2 = :rpc.call(b, ExKV, :get, ["k1"]) 
IO.inspect(res2)

:erlang.halt()
