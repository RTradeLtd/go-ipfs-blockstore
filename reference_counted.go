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
// limitations under the License
package blockstore

import cid "github.com/ipfs/go-cid"

// CountedBlockstore is a reference counted blockstore
type CountedBlockstore interface {
	CollectGarbage() error
	GetRefCount(cid.Cid) (int64, error)
	Close() error
	Blockstore
}
