defmodule ExKV do
  alias ExKV.Router
  # Public API - top-level convenience functions

  # Here we do: provide a simple `put/2` wrapper that delegates to the router.
  # Here we use: `ExKV.Router.route_put/2` to route the request to the right node.
  def put(key, value), do: Router.route_put(key, value)

  # Here we do: provide a simple `get/1` wrapper that delegates to the router.
  # We decided to use: the router's `route_get/1` which returns the value or nil.
  def get(key), do: Router.route_get(key)

  # Here we do: provide a simple `delete/1` wrapper that delegates to the router.
  # Here we use: `ExKV.Router.route_delete/1` to remove the key from the ring.
  def delete(key), do: Router.route_delete(key)
end
