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

import (
	"context"
	"io"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"go.uber.org/zap"
)

var (
	_ Blockstore = (*RemoteBlockstore)(nil)
)

// RemoteBlockstore storing blocks in remote locations
// without running the blockstore locally. If behind latency
// sensitive networks, it is advisable to wrap this in a cached blockstore
type RemoteBlockstore struct {
	ctx     context.Context
	xclient pb.NodeAPIClient
	logger  *zap.Logger
}

// NewRemoteBlockstore returns a new remote blockstore
func NewRemoteBlockstore(ctx context.Context, logger *zap.Logger, xclient pb.NodeAPIClient) *RemoteBlockstore {
	return &RemoteBlockstore{ctx, xclient, logger.Named("remote.blockstore")}
}

// DeleteBlock is used to delete the block from the remote blockstore.
// This must be forced, as TemporalX does not allow "unforced" deletes
func (rbs *RemoteBlockstore) DeleteBlock(gocid cid.Cid) error {
	_, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_DELETE,
		ReqOpts:     []pb.BSREQOPTS{pb.BSREQOPTS_BS_FORCE},
		Cids:        []string{gocid.String()},
	})
	if err != nil {
		rbs.logger.Error("delete block failed", zap.Error(err), zap.String("cid", gocid.String()))
	}
	return err
}

// Has is used to check whether or not we have the block
func (rbs *RemoteBlockstore) Has(gocid cid.Cid) (bool, error) {
	resp, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_HAS,
		Cids:        []string{gocid.String()},
	})
	if err != nil {
		return false, err
	}
	for _, block := range resp.GetBlocks() {
		if block.Cid == gocid.String() {
			return true, nil
		}
	}
	return false, ErrNotFound
}

// Get is used to retrieve a block from our blockstore
func (rbs *RemoteBlockstore) Get(gocid cid.Cid) (blocks.Block, error) {
	resp, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_GET,
		Cids:        []string{gocid.String()},
	})
	if er := rbs.blockNotFound(resp, err); er != nil {
		rbs.logger.Error("get failed", zap.Error(err), zap.String("cid", gocid.String()))
		return nil, er
	}
	return blocks.NewBlockWithCid(resp.GetBlocks()[0].GetData(), gocid)
}

// GetSize returns the size of the block
func (rbs *RemoteBlockstore) GetSize(gocid cid.Cid) (int, error) {
	resp, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_GET_STATS,
		Cids:        []string{gocid.String()},
	})
	if er := rbs.blockNotFound(resp, err); er != nil {
		rbs.logger.Error("get size failed", zap.Error(err), zap.String("cid", gocid.String()))
		return 0, er
	}
	return resp.GetBlocks()[0].Size(), nil
}

// Put allows putting blocks into the remote blockstore
func (rbs *RemoteBlockstore) Put(block blocks.Block) error {
	_, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_PUT,
		Data:        [][]byte{block.RawData()},
	})
	if err != nil {
		rbs.logger.Error("put failed", zap.Error(err))
	}
	return err
}

// PutMany allows putting many blocks into the remote blockstore
func (rbs *RemoteBlockstore) PutMany(blocks []blocks.Block) error {
	// to prevent issues with gRPC max message size, send 1 by 1
	for _, block := range blocks {
		if err := rbs.Put(block); err != nil {
			// done log as put handles the log
			return err
		}
	}
	return nil
}

// AllKeysChan is used to return all keys from the remote blockstore
func (rbs *RemoteBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	stream, err := rbs.xclient.BlockstoreStream(ctx)
	if err != nil {
		rbs.logger.Error("allkeyschan failed", zap.Error(err))
		return nil, err
	}
	if err := stream.Send(&pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_GET_ALL,
	}); err != nil {
		return nil, err
	}

	var keysChan = make(chan cid.Cid)
	go func() {
		defer close(keysChan)
		for {
			msg, err := stream.Recv()
			if err != nil {
				// dont log io.EOF as this is always sent
				if err != io.EOF {
					rbs.logger.Error(
						"received error trying to read message from stream",
						zap.Error(err),
					)
				}
				return
			}
			if msg.GetRequestType() != pb.BSREQTYPE_BS_GET_ALL {
				continue
			}
			for _, block := range msg.Blocks {
				gocid, err := cid.Decode(block.Cid)
				if err != nil {
					rbs.logger.Error(
						"received bad block from temporalx, could not decode cid",
						zap.Error(err),
						zap.String("block.cid", block.Cid),
					)
					return
				}
				keysChan <- gocid
			}
		}
	}()
	return keysChan, nil
}

// HashOnRead toggles hash on read
func (rbs *RemoteBlockstore) HashOnRead(enabled bool) {
	_, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: func() pb.BSREQTYPE {
			if enabled {
				return pb.BSREQTYPE_BS_HASH_ON_READ_ENABLE
			}
			return pb.BSREQTYPE_BS_HASH_ON_READ_DISABLE
		}(),
	})
	if err != nil {
		rbs.logger.Error("failed to set hash on read", zap.Error(err))
	}
}

// blockNotFound is a helper function used to check a response, and the request error
// to see if a block was not found, and return the appropriate block not found error
func (rbs *RemoteBlockstore) blockNotFound(resp *pb.BlockstoreResponse, respErr error) error {
	if respErr != nil {
		return respErr
	}
	if len(resp.GetBlocks()) <= 0 {
		return ErrNotFound
	}
	return nil
}
