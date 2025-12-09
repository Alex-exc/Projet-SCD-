if Code.ensure_loaded?(Plug.Router) do
  defmodule ExKV.HTTP.Router do
    @moduledoc """
    Minimal HTTP API for ExKV using Plug/Cowboy.

    - Here we will do: expose simple REST endpoints for `put`, `get` and `delete`.
    - Here we will use: `ExKV.Router` to perform routing/replication logic.
    - We decided to encode values as Erlang terms (binary) to keep the example simple.
    """

    use Plug.Router

    plug :match
    plug :dispatch

    # PUT /kv/:key
    # Here we will do: read the request body, decode the Erlang term and write it.
    # Here we will use: `ExKV.Router.put/2` which performs replication and returns ack or error.
    post "/kv/:key" do
      {:ok, body, _} = Plug.Conn.read_body(conn)
      value = :erlang.binary_to_term(body)

      case ExKV.Router.put(key, value) do
        {:ok, _ack} -> send_resp(conn, 200, "ok")
        {:error, _} -> send_resp(conn, 500, "write_error")
      end
    end

    # GET /kv/:key
    # Here we will do: ask the router for the value and return it encoded as binary.
    # Here we will use: `ExKV.Router.get/1` which implements read-quorum and fallback.
    get "/kv/:key" do
      case ExKV.Router.get(key) do
        {:ok, val} ->
          send_resp(conn, 200, :erlang.term_to_binary(val))
        {:error, :not_found} -> send_resp(conn, 404, "not_found")
        {:error, _} -> send_resp(conn, 500, "error")
      end
    end

    # DELETE /kv/:key
    # Here we will do: ask the router to delete the key on primary+replicas.
    # Here we will use: `ExKV.Router.delete/1` which performs replicated deletes and hints failures.
    delete "/kv/:key" do
      case ExKV.Router.delete(key) do
        {:ok, _} -> send_resp(conn, 200, "ok")
        _ -> send_resp(conn, 500, "error")
      end
    end

    # Fallback for unknown routes
    match _ do
      send_resp(conn, 404, "unknown")
    end
  end
else
  defmodule ExKV.HTTP.Router do
    @moduledoc false

    # Minimal stub so the project can compile when Plug is not available.
    def child_spec(_opts), do: :ignore
  end
end
