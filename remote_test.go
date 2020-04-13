package blockstore

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	sdkc "github.com/RTradeLtd/go-temporalx-sdk/client"
	blocks "github.com/ipfs/go-block-format"
	"go.uber.org/zap/zaptest"
)

func TestRemoteBlockstore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := os.Getenv("TEST_XAPI")
	if addr == "" {
		addr = "xapi.temporal.cloud:9090"
	}
	client, err := sdkc.NewClient(sdkc.Opts{Insecure: true, ListenAddress: addr})
	if err != nil {
		t.Fatal(err)
	}
	blk := blocks.NewBlock([]byte("hello world"))
	blockstore := NewRemoteBlockstore(ctx, zaptest.NewLogger(t), client.NodeAPIClient)
	t.Run("DeleteBlock", func(t *testing.T) {
		blockstore.DeleteBlock(blk.Cid())
	})
	t.Run("Has", func(t *testing.T) {
		t.Skip("not available in public not yet")
		has, err := blockstore.Has(blk.Cid())
		if err != nil && err != ErrNotFound {
			t.Error(err)
		}
		if has {
			t.Fatal("should not have block")
		}
	})
	t.Run("Put/Get", func(t *testing.T) {
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
	})
	t.Run("GetSize", func(t *testing.T) {
		_, err := blockstore.GetSize(blk.Cid())
		if err != nil {
			t.Fatal(err)
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
		t.Skip("not available in public yet")
		blockstore.HashOnRead(true)
		blockstore.HashOnRead(false)
	})
}
