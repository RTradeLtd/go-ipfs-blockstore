package blockstore

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	xtestutils "github.com/RTradeLtd/TxPB/v3/go/testutils"
	sdkc "github.com/RTradeLtd/go-temporalx-sdk/client"
	blocks "github.com/ipfs/go-block-format"
	"go.uber.org/zap/zaptest"
)

func TestRemoteBlockstore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := sdkc.NewClient(
		sdkc.Opts{
			Insecure:      xtestutils.GetXAPISecure(t),
			ListenAddress: xtestutils.GetXAPIAddress(t),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	blk := blocks.NewBlock([]byte("hello world"))
	blockstore := NewRemoteBlockstore(ctx, zaptest.NewLogger(t), client.NodeAPIClient)
	t.Run("DeleteBlock", func(t *testing.T) {
		blockstore.DeleteBlock(blk.Cid())
	})
	t.Run("Has", func(t *testing.T) {
		has, err := blockstore.Has(blk.Cid())
		if err != nil && err != ErrNotFound {
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
			t.Fatal("should not have block")
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
			blocks.NewBlock(
				[]byte(
					fmt.Sprintf(
						"%v-thisissometestdata-%s",
						rand.Int63n(10000),
						time.Now().String(),
					),
				),
			).Cid(),
		)
		if err == nil {
			t.Fatal("error expected")
		}
		if err := blockstore.PutMany(
			[]blocks.Block{
				blk,
				blocks.NewBlock([]byte("hello world")),
			},
		); err != nil {
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
		blockstore.HashOnRead(false)
	})
}
