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
	if len(resp.GetBlocks()) <= 0 {
		return false, ErrNotFound
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
	if err != nil {
		return nil, err
	}
	if len(resp.GetBlocks()) <= 0 {
		return nil, ErrNotFound
	}
	return blocks.NewBlockWithCid(resp.GetBlocks()[0].GetData(), gocid)
}

// GetSize returns the size of the block
func (rbs *RemoteBlockstore) GetSize(gocid cid.Cid) (int, error) {
	resp, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_GET_STATS,
		Cids:        []string{gocid.String()},
	})
	if err != nil {
		return 0, err
	}
	if len(resp.GetBlocks()) <= 0 {
		return 0, ErrNotFound
	}
	return resp.GetBlocks()[0].Size(), nil
}

// Put allows putting blocks into the remote blockstore
func (rbs *RemoteBlockstore) Put(block blocks.Block) error {
	_, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_PUT,
		Data:        [][]byte{block.RawData()},
	})
	return err
}

// PutMany allows putting many blocks into the remote blockstore
func (rbs *RemoteBlockstore) PutMany(blocks []blocks.Block) error {
	var dataSlice = make([][]byte, len(blocks))
	for i, block := range blocks {
		dataSlice[i] = block.RawData()
	}
	_, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_PUT,
		Data:        dataSlice,
	})
	return err
}

// AllKeysChan is used to return all keys from the remote blockstore
func (rbs *RemoteBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	stream, err := rbs.xclient.BlockstoreStream(ctx)
	if err != nil {
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
