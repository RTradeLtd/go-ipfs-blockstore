// Copyright 2020 RTrade Technologies Ltd
//
// licensed under GNU AFFERO GENERAL PUBLIC LICENSE;
// you may not use this file except in compliance with the License;
// You may obtain the license via the LICENSE file in the repository root;
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
