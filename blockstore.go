package blockstore

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
	ib "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	uatomic "go.uber.org/atomic"
	"go.uber.org/zap"
)

// BlockPrefix namespaces blockstore datastores
var BlockPrefix = ib.BlockPrefix

// ErrHashMismatch is an error returned when the hash of a block
// is different than expected.
var ErrHashMismatch = ib.ErrHashMismatch

// ErrNotFound is an error returned when a block is not found.
var ErrNotFound = ib.ErrNotFound

// Blockstore aliases upstream blockstore interface
type Blockstore = ib.Blockstore

// GCLocker aliases upstream gclocker interface
type GCLocker = ib.GCLocker

// GCBlockstore aliases upstream gcblockstore interface
type GCBlockstore = ib.GCBlockstore

// Unlocker aliases upstream unlocker interface
type Unlocker = ib.Unlocker

// NewBlockstore returns a default Blockstore implementation
// using the provided datastore.Batching backend.
func NewBlockstore(logger *zap.Logger, d ds.Batching) Blockstore {
	var dsb ds.Batching
	dd := dsns.Wrap(d, BlockPrefix)
	dsb = dd
	return &blockstore{
		datastore: dsb,
		logger:    logger.Named("blockstore"),
		rehash:    uatomic.NewBool(false),
	}
}

type blockstore struct {
	datastore ds.Batching
	logger    *zap.Logger
	rehash    *uatomic.Bool
}

func (bs *blockstore) HashOnRead(enabled bool) {
	bs.rehash.Store(enabled)
}

func (bs *blockstore) Get(k cid.Cid) (blocks.Block, error) {
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

func (bs *blockstore) Put(block blocks.Block) error {
	k := dshelp.MultihashToDsKey(block.Cid().Hash())

	// Has is cheaper than Put, so see if we already have it
	exists, err := bs.datastore.Has(k)
	if err == nil && exists {
		return nil // already stored.
	}
	return bs.datastore.Put(k, block.RawData())
}

func (bs *blockstore) PutMany(blocks []blocks.Block) error {
	t, err := bs.datastore.Batch()
	if err != nil {
		return err
	}
	for _, b := range blocks {
		k := dshelp.MultihashToDsKey(b.Cid().Hash())
		exists, err := bs.datastore.Has(k)
		if err == nil && exists {
			continue
		}

		err = t.Put(k, b.RawData())
		if err != nil {
			return err
		}
	}
	return t.Commit()
}

func (bs *blockstore) Has(k cid.Cid) (bool, error) {
	return bs.datastore.Has(dshelp.MultihashToDsKey(k.Hash()))
}

func (bs *blockstore) GetSize(k cid.Cid) (int, error) {
	size, err := bs.datastore.GetSize(dshelp.MultihashToDsKey(k.Hash()))
	if err == ds.ErrNotFound {
		return -1, ErrNotFound
	}
	return size, err
}

func (bs *blockstore) DeleteBlock(k cid.Cid) error {
	return bs.datastore.Delete(dshelp.MultihashToDsKey(k.Hash()))
}

// AllKeysChan runs a query for keys from the blockstore.
// this is very simplistic, in the future, take dsq.Query as a param?
//
// AllKeysChan respects context.
func (bs *blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {

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
