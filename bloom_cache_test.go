package blockstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	syncds "github.com/ipfs/go-datastore/sync"
	ib "github.com/ipfs/go-ipfs-blockstore"
	"go.uber.org/zap/zaptest"
)

func testBloomCached(ctx context.Context, bs ib.Blockstore) (*bloomcache, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts := DefaultCacheOpts()
	opts.HasARCCacheSize = 0
	bbs, err := CachedBlockstore(ctx, bs, opts)
	if err == nil {
		return bbs.(*bloomcache), nil
	}
	return nil, err
}

func TestPutManyAddsToBloom(t *testing.T) {
	bs := NewBlockstore(zaptest.NewLogger(t), syncds.MutexWrap(ds.NewMapDatastore()))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("Failed while waiting for the filter to build: %d", cachedbs.bloom.ElementsAdded())
	}

	block1 := blocks.NewBlock([]byte("foo"))
	block2 := blocks.NewBlock([]byte("bar"))
	emptyBlock := blocks.NewBlock([]byte{})

	cachedbs.PutMany([]blocks.Block{block1, emptyBlock})
	has, err := cachedbs.Has(block1.Cid())
	if err != nil {
		t.Fatal(err)
	}
	blockSize, err := cachedbs.GetSize(block1.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if blockSize == -1 || !has {
		t.Fatal("added block is reported missing")
	}

	has, err = cachedbs.Has(block2.Cid())
	if err != nil {
		t.Fatal(err)
	}
	blockSize, err = cachedbs.GetSize(block2.Cid())
	if err != nil && err != ErrNotFound {
		t.Fatal(err)
	}
	if blockSize > -1 || has {
		t.Fatal("not added block is reported to be in blockstore")
	}

	has, err = cachedbs.Has(emptyBlock.Cid())
	if err != nil {
		t.Fatal(err)
	}
	blockSize, err = cachedbs.GetSize(emptyBlock.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if blockSize != 0 || !has {
		t.Fatal("added block is reported missing")
	}
}

func TestReturnsErrorWhenSizeNegative(t *testing.T) {
	bs := NewBlockstore(zaptest.NewLogger(t), syncds.MutexWrap(ds.NewMapDatastore()))
	_, err := bloomCached(context.Background(), bs, -1, 1)
	if err == nil {
		t.Fail()
	}
}
func TestHasIsBloomCached(t *testing.T) {
	cd := &callbackDatastore{f: func() {}, ds: ds.NewMapDatastore()}
	bs := NewBlockstore(zaptest.NewLogger(t), syncds.MutexWrap(cd))

	for i := 0; i < 1000; i++ {
		bs.Put(blocks.NewBlock([]byte(fmt.Sprintf("data: %d", i))))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("Failed while waiting for the filter to build: %d", cachedbs.bloom.ElementsAdded())
	}

	cacheFails := 0
	cd.SetFunc(func() {
		cacheFails++
	})

	for i := 0; i < 1000; i++ {
		cachedbs.Has(blocks.NewBlock([]byte(fmt.Sprintf("data: %d", i+2000))).Cid())
	}

	if float64(cacheFails)/float64(1000) > float64(0.05) {
		t.Fatalf("Bloom filter has cache miss rate of more than 5%%")
	}

	cacheFails = 0
	block := blocks.NewBlock([]byte("newBlock"))

	cachedbs.PutMany([]blocks.Block{block})
	if cacheFails != 2 {
		t.Fatalf("expected two datastore hits: %d", cacheFails)
	}
	cachedbs.Put(block)
	if cacheFails != 3 {
		t.Fatalf("expected datastore hit: %d", cacheFails)
	}

	if has, err := cachedbs.Has(block.Cid()); !has || err != nil {
		t.Fatal("has gave wrong response")
	}

	bl, err := cachedbs.Get(block.Cid())
	if bl.String() != block.String() {
		t.Fatal("block data doesn't match")
	}

	if err != nil {
		t.Fatal("there should't be an error")
	}
}

var _ ds.Batching = (*callbackDatastore)(nil)

type callbackDatastore struct {
	sync.Mutex
	f  func()
	ds ds.Datastore
}

func (c *callbackDatastore) SetFunc(f func()) {
	c.Lock()
	defer c.Unlock()
	c.f = f
}

func (c *callbackDatastore) CallF() {
	c.Lock()
	defer c.Unlock()
	c.f()
}

func (c *callbackDatastore) Put(key ds.Key, value []byte) (err error) {
	c.CallF()
	return c.ds.Put(key, value)
}

func (c *callbackDatastore) Get(key ds.Key) (value []byte, err error) {
	c.CallF()
	return c.ds.Get(key)
}

func (c *callbackDatastore) Has(key ds.Key) (exists bool, err error) {
	c.CallF()
	return c.ds.Has(key)
}

func (c *callbackDatastore) GetSize(key ds.Key) (size int, err error) {
	c.CallF()
	return c.ds.GetSize(key)
}

func (c *callbackDatastore) Close() error {
	return nil
}

func (c *callbackDatastore) Delete(key ds.Key) (err error) {
	c.CallF()
	return c.ds.Delete(key)
}

func (c *callbackDatastore) Query(q dsq.Query) (dsq.Results, error) {
	c.CallF()
	return c.ds.Query(q)
}

func (c *callbackDatastore) Sync(key ds.Key) error {
	c.CallF()
	return c.ds.Sync(key)
}

func (c *callbackDatastore) Batch() (ds.Batch, error) {
	return ds.NewBasicBatch(c), nil
}
