defmodule ExKV.Router do
  use Plug.Router
  require Logger

  plug :match
  plug Plug.Parsers,
    parsers: [:json],
    json_decoder: Jason
  plug :dispatch

  # -------------------------
  # ROUTES
  # -------------------------

  get "/get/:key" do
    handle_request("GET", key, nil, conn)
  end

  post "/put" do
    %{"key" => key, "value" => value} = conn.body_params
    handle_request("PUT", key, value, conn)
  end

  delete "/delete/:key" do
    handle_request("DELETE", key, nil, conn)
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

  # -------------------------
  # REQUEST DISPATCH
  # -------------------------

  def handle_request(method, key, value, conn) do
    nodes = ExKV.NodeRegistry.nodes()
    ring = ExKV.HashRing.build(nodes)
    target = ExKV.HashRing.find_node(key, ring)

    local = "http://localhost:4000"

    if target == local do
      process_local(method, key, value, conn)
    else
      forward_request(method, key, value, conn, target)
    end
  end

  # -------------------------
  # LOCAL PROCESSING
  # -------------------------

  def process_local("GET", key, _value, conn) do
    value = ExKV.Store.get(String.to_atom(key))
    send_resp(conn, 200, Jason.encode!(%{key: key, value: value}))
  end

  def process_local("PUT", key, value, conn) do
    # 1. Write locally
    ExKV.Store.put(String.to_atom(key), value)

    # 2. Replication (factor = 2)
    nodes = ExKV.NodeRegistry.nodes()
    ring = ExKV.HashRing.build(nodes)
    replicas = ExKV.HashRing.replication_nodes(key, ring, 2)

    for node <- replicas do
      if node != "http://localhost:4000" do
        Req.post("#{node}/put", json: %{key: key, value: value})
      end
    end

    send_resp(conn, 200, Jason.encode!(%{status: "ok"}))
  end

  def process_local("DELETE", key, _value, conn) do
    ExKV.Store.delete(String.to_atom(key))
    send_resp(conn, 200, Jason.encode!(%{status: "deleted"}))
  end

  # -------------------------
  # FORWARD TO OTHER NODES
  # -------------------------

  def forward_request("GET", key, _value, conn, node) do
    url = "#{node}/get/#{key}"
    {:ok, response} = Req.get(url)
    send_resp(conn, 200, response.body)
  end

  def forward_request("PUT", key, value, conn, node) do
    url = "#{node}/put"
    {:ok, response} = Req.post(url, json: %{key: key, value: value})
    send_resp(conn, 200, response.body)
  end

  def forward_request("DELETE", key, _value, conn, node) do
    url = "#{node}/delete/#{key}"
    {:ok, response} = Req.delete(url)
    send_resp(conn, 200, response.body)
  end
end
