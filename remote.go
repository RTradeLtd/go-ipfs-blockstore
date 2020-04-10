package blockstore

import (
	"context"
	"errors"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

// RemoteBlockstore storing blocks in remote locations
// without running the blockstore locally.
type RemoteBlockstore struct {
	ctx     context.Context
	xclient pb.NodeAPIClient
}

// NewRemoteBlockstore returns a new remote blockstore
func NewRemoteBlockstore(ctx context.Context, xclient pb.NodeAPIClient) *RemoteBlockstore {
	return &RemoteBlockstore{ctx, xclient}
}

func (rbs *RemoteBlockstore) Has(gocid cid.Cid) (bool, error) {
	return false, errors.New("not yet implemented")
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
		return nil, errors.New("block not found")
	}
	return blocks.NewBlockWithCid(resp.GetBlocks()[0].GetData(), gocid)
}

func (rbs *RemoteBlockstore) Put(block blocks.Block) error {
	_, err := rbs.xclient.Blockstore(rbs.ctx, &pb.BlockstoreRequest{
		RequestType: pb.BSREQTYPE_BS_PUT,
		Data:        [][]byte{block.RawData()},
	})
	return err
}

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
		for {
			msg, err := stream.Recv()
			if err != nil {
				// TODO(bonedaddy): log
				return
			}
			if msg.GetRequestType() != pb.BSREQTYPE_BS_GET_ALL {
				continue
			}
			for _, block := range msg.Blocks {
				gocid, err := cid.Decode(block.Cid)
				if err != nil {
					// TODO(bonedaddy): log
					return
				}
				keysChan <- gocid
			}
		}
	}()
	return keysChan, nil
}
