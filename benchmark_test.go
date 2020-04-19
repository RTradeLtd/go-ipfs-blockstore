package blockstore

import (
	"fmt"
	"os"
	"testing"

	badger "github.com/RTradeLtd/go-datastores/badger"
	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"go.uber.org/zap"
)

type nilLogger struct{}

func (n nilLogger) Errorf(string, ...interface{}) {
	return
}

func (n nilLogger) Warningf(string, ...interface{}) {
	return
}

func (n nilLogger) Infof(string, ...interface{}) {
	return
}

func (n nilLogger) Debugf(string, ...interface{}) {
	return
}

func BenchmarkMemoryBlockstorePut(b *testing.B) {
	b.StopTimer()
	bs := NewBlockstore(zap.NewNop(), ds_sync.MutexWrap(ds.NewMapDatastore()))
	var blks = make([]blocks.Block, b.N)
	for i := 0; i < b.N; i++ {
		blks[i] = blocks.NewBlock([]byte(fmt.Sprint(i)))
	}
	b.StartTimer()
	// test fresh put
	for i := 0; i < b.N; i++ {
		bs.Put(blks[i])
	}
	// test old put
	for i := 0; i < b.N; i++ {
		bs.Put(blks[i])
	}
}

func BenchmarkBadgerBlockstorePut(b *testing.B) {
	b.StopTimer()
	opts := badger.DefaultOptions
	opts.Logger = nilLogger{}
	bds, err := badger.NewDatastore("mytestdir", &opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		bds.Close()
		os.RemoveAll("mytestdir")
	})
	bs := NewBlockstore(zap.NewNop(), bds)
	var blks = make([]blocks.Block, b.N)
	for i := 0; i < b.N; i++ {
		blks[i] = blocks.NewBlock([]byte(fmt.Sprint(i)))
	}
	b.StartTimer()
	// test fresh put
	for i := 0; i < b.N; i++ {
		bs.Put(blks[i])
	}
	// test old put
	for i := 0; i < b.N; i++ {
		bs.Put(blks[i])
	}
	b.StopTimer()
}

func BenchmarkMemoryExperimentalBlockstorePut(b *testing.B) {
	b.StopTimer()
	bs := NewExperimentalBlockstore(zap.NewNop(), ds_sync.MutexWrap(ds.NewMapDatastore()))
	var blks = make([]blocks.Block, b.N)
	for i := 0; i < b.N; i++ {
		blks[i] = blocks.NewBlock([]byte(fmt.Sprint(i)))
	}
	b.StartTimer()
	// test fresh put
	for i := 0; i < b.N; i++ {
		bs.Put(blks[i])
	}
	// test old put
	for i := 0; i < b.N; i++ {
		bs.Put(blks[i])
	}
}

func BenchmarkBadgerExperimentalBlockstorePut(b *testing.B) {
	b.StopTimer()
	opts := badger.DefaultOptions
	opts.Logger = nilLogger{}
	bds, err := badger.NewDatastore("mytestdir", &opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		bds.Close()
		os.RemoveAll("mytestdir")
	})
	bs := NewExperimentalBlockstore(zap.NewNop(), bds)
	var blks = make([]blocks.Block, b.N)
	for i := 0; i < b.N; i++ {
		blks[i] = blocks.NewBlock([]byte(fmt.Sprint(i)))
	}
	b.StartTimer()
	// test fresh put
	for i := 0; i < b.N; i++ {
		bs.Put(blks[i])
	}
	// test old put
	for i := 0; i < b.N; i++ {
		bs.Put(blks[i])
	}
	b.StopTimer()
}
