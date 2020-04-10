module github.com/ipfs/go-ipfs-blockstore

require (
	github.com/RTradeLtd/TxPB/v3 v3.2.3-0.20200403061323-e393bc5d3fa8
	github.com/hashicorp/golang-lru v0.5.4
	github.com/ipfs/bbloom v0.0.4
	github.com/ipfs/go-block-format v0.0.2
	github.com/ipfs/go-cid v0.0.5
	github.com/ipfs/go-datastore v0.4.2
	github.com/ipfs/go-ipfs-ds-help v1.0.0
	github.com/ipfs/go-ipfs-util v0.0.1
	github.com/ipfs/go-log v0.0.1
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/multiformats/go-multihash v0.0.13
)

go 1.13

replace github.com/RTradeLtd/TxPB/v3 => ../TxPB
