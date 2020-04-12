package blockstore

import "github.com/prometheus/client_golang/prometheus"

func init() {
	prometheus.MustRegister(arcCacheHits)
	prometheus.MustRegister(arcCacheRequests)
	prometheus.MustRegister(bloomCacheHits)
	prometheus.MustRegister(bloomCacheRequests)
}

var (
	arcCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "blockstore",
		Subsystem: "arc_cache",
		Name:      "hits",
		Help:      "tracks total number of ARC cache hits",
	})
	arcCacheRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "blockstore",
		Subsystem: "arc_cache",
		Name:      "requests_total",
		Help:      "tracks the total number of ARC cache requests",
	})
	bloomCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "blockstore",
		Subsystem: "bloom_filter",
		Name:      "hits_cache",
		Help:      "tracks total number of bloom filter hits",
	})
	bloomCacheRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "blockstore",
		Subsystem: "bloom_filter",
		Name:      "hits_total",
		Help:      "tracks the total number of bloom filter requests",
	})
)
