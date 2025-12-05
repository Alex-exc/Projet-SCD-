defmodule ExKV.Application do

  use Application

  ## ------------------------------------------------------------------
  ## Application Entry Point
  ## ------------------------------------------------------------------
  # The main entry point for the node-local supervision tree.
  # We use a `:one_for_one` strategy because each child process can fail
  # independently; restarting one child does not require restarting the others.
  @impl true
  def start(_type, _args) do
    ## --------------------------------------------------------------
    ## Core system children
    ## --------------------------------------------------------------

    base_children = [
      # Ring manager: maintains the consistent-hash ring and node membership.
      # Centralizes cluster topology updates and provides lookup APIs.
      ExKV.RingManager,

      # Local storage backend: manages key/value persistence.
      # Uses DETS if available; maintains a fast in-memory cache for reads.
      # Registered as :store to ensure one instance per node.
      {ExKV.Store, name: :store, file: "./exkv_store.dets"},

      # Hinted handoff manager: buffers writes for temporarily unreachable nodes.
      # Ensures eventual consistency by retrying delivery asynchronously.
      ExKV.HandoffManager,

      # Anti-entropy worker: periodically compares local keys with other nodes
      # and reconciles differences to ensure replication convergence.
      ExKV.AntiEntropy,

      # Configuration holder: centralizes runtime configuration and parameters.
      # Lightweight, stateless, and used by other components to avoid scattering configs.
      ExKV.Config
    ]

    ## --------------------------------------------------------------
    ## Optional HTTP API children
    ## --------------------------------------------------------------
    # If Plug.Cowboy is available, start the HTTP endpoint for remote access.
    # This is kept optional to allow running the storage system without a web interface.
    http_children =
      if Code.ensure_loaded?(Plug.Cowboy) do
        [
          {Plug.Cowboy,
           scheme: :http,
           plug: ExKV.HTTP.Router,
           options: [port: 4000]}
        ]
      else
        []
      end

    ## --------------------------------------------------------------
    ## Combine all children into the final supervision list
    ## --------------------------------------------------------------
    children = base_children ++ http_children

    ## --------------------------------------------------------------
    ## Supervisor options
    ## --------------------------------------------------------------
    opts = [
      strategy: :one_for_one, # Restart each child independently on failure
      name: ExKV.Supervisor    # Globally registered supervisor name
    ]

    ## --------------------------------------------------------------
    ## Start the supervision tree
    ## --------------------------------------------------------------
    Supervisor.start_link(children, opts)
  end
end
