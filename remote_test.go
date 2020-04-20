package blockstore

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	xtestutils "github.com/RTradeLtd/TxPB/v3/go/xtestutils"
	sdkc "github.com/RTradeLtd/go-temporalx-sdk/client"
	blocks "github.com/ipfs/go-block-format"
	"go.uber.org/zap/zaptest"
)

func TestRemoteBlockstore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := sdkc.NewClient(
		sdkc.Opts{
			Insecure:      xtestutils.GetXAPIInsecure(t),
			ListenAddress: xtestutils.GetXAPIAddress(t),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	blk := blocks.NewBlock(getBlockContents(t, 1))
	blockstore := NewRemoteBlockstore(ctx, zaptest.NewLogger(t), client.NodeAPIClient)
	t.Run("DeleteBlock", func(t *testing.T) {
		if err := blockstore.DeleteBlock(blk.Cid()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Has", func(t *testing.T) {
		has, err := blockstore.Has(blk.Cid())
		if err != ErrNotFound {
			t.Error(err)
		}
		if has {
			t.Fatal("should not have block")
		}
		if err := blockstore.Put(blk); err != nil {
			t.Fatal(err)
		}
		has, err = blockstore.Has(blk.Cid())
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatal("should have block")
		}
	})
	t.Run("Put/Get/PutMany", func(t *testing.T) {
		if err := blockstore.Put(blk); err != nil {
			t.Fatal(err)
		}
		retBlk, err := blockstore.Get(blk.Cid())
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(blk, retBlk) {
			t.Fatal("bad block retrieved")
		}
		_, err = blockstore.Get(
			blocks.NewBlock(getBlockContents(t, 2)).Cid(),
		)
		if err == nil {
			t.Fatal("error expected")
		}
		if err := blockstore.PutMany(
			[]blocks.Block{
				blk,
				blocks.NewBlock(getBlockContents(t, 3)),
			},
		); err != nil {
			t.Fatal(err)
		}
		// test 0 block put
		if err := blockstore.PutMany(nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("GetSize", func(t *testing.T) {
		sz, err := blockstore.GetSize(blk.Cid())
		if err != nil {
			t.Fatal(err)
		}
		if sz != 50 {
			t.Fatal("bad size returned")
		}
	})
	t.Run("AllKeysChan", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		ch, err := blockstore.AllKeysChan(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for range ch {

		}
	})
	t.Run("HashOnRead", func(t *testing.T) {
		blockstore.HashOnRead(true)
		if _, err := blockstore.Get(blk.Cid()); err != nil {
			t.Fatal(err)
		}
		blockstore.HashOnRead(false)
		if _, err := blockstore.Get(blk.Cid()); err != nil {
			t.Fatal(err)
		}
	})
}

func getBlockContents(t *testing.T, count int) []byte {
	t.Helper()
	return []byte(
		fmt.Sprintf(
			"%s-go-ipfs-blockstore_remote-%v-%v-%v",
			time.Now().String(),
			rand.Int63n(math.MaxInt64),
			time.Now().UnixNano(),
			count,
		),
	)
}
