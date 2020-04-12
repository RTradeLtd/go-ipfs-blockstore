// Package blockstore implements a thin wrapper over a datastore, giving a
// clean interface for Getting and Putting block objects.
package blockstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
	ib "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("blockstore")

// BlockPrefix namespaces blockstore datastores
var BlockPrefix = ds.NewKey("blocks")

// ErrHashMismatch is an error returned when the hash of a block
// is different than expected.
var ErrHashMismatch = errors.New("block in storage has different hash than requested")

// ErrNotFound is an error returned when a block is not found.
var ErrNotFound = errors.New("blockstore: block not found")

// Blockstore aliases upstream blockstore interface
type Blockstore ib.Blockstore

// GCLocker aliases upstream gclocker interface
type GCLocker ib.GCLocker

// GCBlockstore aliases upstream gcblockstore interface
type GCBlockstore ib.GCBlockstore

// Unlocker aliases upstream unlocker interface
type Unlocker ib.Unlocker

// NewGCBlockstore returns a default implementation of GCBlockstore
// using the given Blockstore and GCLocker.
func NewGCBlockstore(bs Blockstore, gcl GCLocker) GCBlockstore {
	return gcBlockstore{bs, gcl}
}

type gcBlockstore struct {
	Blockstore
	GCLocker
}

// NewBlockstore returns a default Blockstore implementation
// using the provided datastore.Batching backend.
func NewBlockstore(d ds.Batching) ib.Blockstore {
	var dsb ds.Batching
	dd := dsns.Wrap(d, BlockPrefix)
	dsb = dd
	return &blockstore{
		datastore: dsb,
	}
}

type blockstore struct {
	datastore ds.Batching

	rehash bool
}

func (bs *blockstore) HashOnRead(enabled bool) {
	bs.rehash = enabled
}

func (bs *blockstore) Get(k cid.Cid) (blocks.Block, error) {
	if !k.Defined() {
		log.Error("undefined cid in blockstore")
		return nil, ErrNotFound
	}
	bdata, err := bs.datastore.Get(dshelp.MultihashToDsKey(k.Hash()))
	if err == ds.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if bs.rehash {
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
				log.Errorf("blockstore.AllKeysChan got err: %s", e.Error)
				return
			}

			// need to convert to key.Key using key.KeyFromDsKey.
			bk, err := dshelp.BinaryFromDsKey(ds.RawKey(e.Key))
			if err != nil {
				log.Warningf("error parsing key from binary: %s", err)
				continue
			}
			//k := cid.NewCidV1(cid.Raw, bk)
			k, err := cid.Cast(bk)
			if err != nil {
				log.Warningf("failed to cast cid: %s", err)
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

// NewGCLocker returns a default implementation of
// GCLocker using standard [RW] mutexes.
func NewGCLocker() GCLocker {
	return &gclocker{}
}

type gclocker struct {
	lk    sync.RWMutex
	gcreq int32
}

type unlocker struct {
	unlock func()
}

func (u *unlocker) Unlock() {
	u.unlock()
	u.unlock = nil // ensure its not called twice
}

func (bs *gclocker) GCLock() ib.Unlocker {
	atomic.AddInt32(&bs.gcreq, 1)
	bs.lk.Lock()
	atomic.AddInt32(&bs.gcreq, -1)
	return &unlocker{bs.lk.Unlock}
}

func (bs *gclocker) PinLock() ib.Unlocker {
	bs.lk.RLock()
	return &unlocker{bs.lk.RUnlock}
}

func (bs *gclocker) GCRequested() bool {
	return atomic.LoadInt32(&bs.gcreq) > 0
}
