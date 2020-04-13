package blockstore

import (
	"context"
	"os"
	"testing"

	sdkc "github.com/RTradeLtd/go-temporalx-sdk/client"
	blocks "github.com/ipfs/go-block-format"
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
	blockstore := NewRemoteBlockstore(ctx, client.NodeAPIClient)
	t.Run("DeleteBlock", func(t *testing.T) {
		blk := blocks.NewBlock([]byte("hello world"))
		blockstore.DeleteBlock(blk.Cid())
	})
}
