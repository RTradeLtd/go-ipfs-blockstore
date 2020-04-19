package blockstore

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	uatomic "go.uber.org/atomic"
	"go.uber.org/zap"
)

// ExperimentalBlockstore is an experiment for
// a faster blockstore
type ExperimentalBlockstore struct {
	datastore ds.Batching
	logger    *zap.Logger
	rehash    *uatomic.Bool
}

// NewExperimentalBlockstore returns a default Blockstore implementation
// using the provided datastore.Batching backend.
func NewExperimentalBlockstore(logger *zap.Logger, d ds.Batching) Blockstore {
	return &blockstore{
		datastore: dsns.Wrap(d, BlockPrefix),
		logger:    logger.Named("blockstore"),
		rehash:    uatomic.NewBool(false),
	}
}

// HashOnRead toggles hash verification of blocks on read
func (bs *ExperimentalBlockstore) HashOnRead(enabled bool) {
	bs.rehash.Store(enabled)
}

// Get returns a blocks from the blockstore
func (bs *ExperimentalBlockstore) Get(k cid.Cid) (blocks.Block, error) {
	if !k.Defined() {
		bs.logger.Error("undefined cid in blockstore")
		return nil, ErrNotFound
	}
	bdata, err := bs.datastore.Get(dshelp.MultihashToDsKey(k.Hash()))
	if err == ds.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if bs.rehash.Load() {
		rbcid, err := k.Prefix().Sum(bdata)
		if err != nil {
			return nil, err
		}

		if !rbcid.Equals(k) {
			return nil, ErrHashMismatch
		}

		return blocks.NewBlockWithCid(bdata, rbcid)
	}
	return blocks.NewBlockWithCid(bdata, k)
}

// Put inserts a block into the blockstore
func (bs *ExperimentalBlockstore) Put(block blocks.Block) error {
	return bs.datastore.Put(
		dshelp.MultihashToDsKey(block.Cid().Hash()),
		block.RawData(),
	)
}

// PutMany is used to put multiple blocks into the blockstore
func (bs *ExperimentalBlockstore) PutMany(blocks []blocks.Block) error {
	t, err := bs.datastore.Batch()
	if err != nil {
		return err
	}
	for _, b := range blocks {
		if err = t.Put(
			dshelp.MultihashToDsKey(b.Cid().Hash()),
			b.RawData(),
		); err != nil {
			return err
		}
	}
	return t.Commit()
}

// Has checks to see if we have the block in our blockstore
func (bs *ExperimentalBlockstore) Has(k cid.Cid) (bool, error) {
	return bs.datastore.Has(dshelp.MultihashToDsKey(k.Hash()))
}

// GetSize returns the size of the associated block
func (bs *ExperimentalBlockstore) GetSize(k cid.Cid) (int, error) {
	size, err := bs.datastore.GetSize(dshelp.MultihashToDsKey(k.Hash()))
	if err == ds.ErrNotFound {
		return -1, ErrNotFound
	}
	return size, err
}

// DeleteBlock removes the block from our blockstore
func (bs *ExperimentalBlockstore) DeleteBlock(k cid.Cid) error {
	return bs.datastore.Delete(dshelp.MultihashToDsKey(k.Hash()))
}

// AllKeysChan runs a query for keys from the blockstore.
// this is very simplistic, in the future, take dsq.Query as a param?
//
// AllKeysChan respects context.
func (bs *ExperimentalBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {

	// KeysOnly, because that would be _a lot_ of data.
	q := dsq.Query{KeysOnly: true}
	res, err := bs.datastore.Query(q)
	if err != nil {
		return nil, err
	}

	output := make(chan cid.Cid, dsq.KeysOnlyBufSize)
	go func() {
		defer func() {
			res.Close() // ensure exit (signals early exit, too)
			close(output)
		}()

		for {
			e, ok := res.NextSync()
			if !ok {
				return
			}
			if e.Error != nil {
				bs.logger.Error("AllKeysChan received error", zap.Error(err))
				return
			}

			// need to convert to key.Key using key.KeyFromDsKey.
			bk, err := dshelp.BinaryFromDsKey(ds.RawKey(e.Key))
			if err != nil {
				bs.logger.Warn("error parsing key from binary", zap.Error(err))
				continue
			}
			// this is commented out from upstream
			// unfortunately it seems like the assumption that
			// this will work even for cidv0 objects is false
			// as we have some tests which generate cidv0 objects
			// that break this
			// k := cid.NewCidV1(cid.Raw, bk)
			k, err := cid.Cast(bk)
			if err != nil {
				bs.logger.Warn("failed to cast cid", zap.Error(err))
			}
			select {
			case <-ctx.Done():
				return
			case output <- k:
			}
		}
	}()

	return output, nil
}
