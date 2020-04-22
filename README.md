# go-ipfs-blockstore


`go-ipfs-blockstore` is a fork of `ipfs/go-ipfs-blockstore` tailored to the needs of TemporalX. List of changes:

* Alias interfaces to upstream
* Remove of `GCBlockstore` implementation, but left the alias for compatability
* Implement logging with `zap` instead of `ipfs/go-log`
* Use prometheus for metrics directly instead of `go-ipfs-metrics`
* Adds an interface type for our reference counted blockstore

# License

All previous code from upstream is licensed under MIT, while our new additions is licensed under AGPL-3.0; All AGPL-3.0 license code has a copyright header at the top of the file. For our actual license see `LICENSE` for upstream license see `LICENSE.orig`