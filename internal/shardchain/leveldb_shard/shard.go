package leveldb_shard

import (
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"path/filepath"
	"strings"
	"sync"
)

type LeveldbShard struct {
	dbs     []*leveldb.DB
	dbCount uint32
}

func NewLeveldbShard(savePath string, diskCount int, diskShards int) (shard *LeveldbShard, err error) {
	shard = &LeveldbShard{
		dbs:     make([]*leveldb.DB, 0, diskCount*diskShards),
		dbCount: uint32(diskCount * diskShards),
	}

	// clean when error
	defer func() {
		if err != nil {
			for _, db := range shard.dbs {
				_ = db.Close()
			}

			shard = nil
		}
	}()

	levelDBOptions := &opt.Options{
		OpenFilesCacheCapacity: 128,
		WriteBuffer:            32 << 20, //32MB
		BlockCacheCapacity:     64 << 20, //64MB
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
	}

	// async open
	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	for i := 0; i < diskCount; i++ {
		for j := 0; j < diskShards; j++ {
			shardPath := filepath.Join(savePath, fmt.Sprintf("disk%02d", i), fmt.Sprintf("block%02d", j))
			wg.Add(1)
			go func() {
				defer wg.Done()

				file, openErr := leveldb.OpenFile(shardPath, levelDBOptions)
				if openErr != nil {
					err = openErr
				}

				lock.Lock()
				defer lock.Unlock()
				shard.dbs = append(shard.dbs, file)
			}()
		}
	}

	wg.Wait()

	return shard, err
}

func (l *LeveldbShard) mapDB(key []byte) *leveldb.DB {
	return l.dbs[mapDBIndex(key, l.dbCount)]
}

// Has retrieves if a key is present in the key-value data store.
func (l *LeveldbShard) Has(key []byte) (bool, error) {
	return l.mapDB(key).Has(key, nil)
}

// Get retrieves the given key if it's present in the key-value data store.
func (l *LeveldbShard) Get(key []byte) ([]byte, error) {
	return l.mapDB(key).Get(key, nil)
}

// Put inserts the given value into the key-value data store.
func (l *LeveldbShard) Put(key []byte, value []byte) error {
	return l.mapDB(key).Put(key, value, nil)
}

// Delete removes the key from the key-value data store.
func (l *LeveldbShard) Delete(key []byte) error {
	return l.mapDB(key).Delete(key, nil)
}

// NewBatch creates a write-only database that buffers changes to its host db
// until a final write is called.
func (l *LeveldbShard) NewBatch() ethdb.Batch {
	return NewLeveldbShardBatch(l)
}

// NewIterator creates a binary-alphabetical iterator over the entire keyspace
// contained within the key-value database.
func (l *LeveldbShard) NewIterator() ethdb.Iterator {
	return l.iterator(nil)
}

// NewIteratorWithStart creates a binary-alphabetical iterator over a subset of
// database content starting at a particular initial key (or after, if it does
// not exist).
func (l *LeveldbShard) NewIteratorWithStart(start []byte) ethdb.Iterator {
	return l.iterator(&util.Range{Start: start})
}

// NewIteratorWithPrefix creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix.
func (l *LeveldbShard) NewIteratorWithPrefix(prefix []byte) ethdb.Iterator {
	return l.iterator(util.BytesPrefix(prefix))
}

func (l *LeveldbShard) iterator(slice *util.Range) ethdb.Iterator {
	iters := make([]iterator.Iterator, l.dbCount)

	for i, db := range l.dbs {
		iter := db.NewIterator(slice, nil)
		iters[i] = iter
	}

	return iterator.NewMergedIterator(iters, comparer.DefaultComparer, true)
}

// Stat returns a particular internal stat of the database.
func (l *LeveldbShard) Stat(property string) (string, error) {
	sb := strings.Builder{}

	for i, db := range l.dbs {
		getProperty, err := db.GetProperty(property)
		if err != nil {
			return "", err
		}

		sb.WriteString(fmt.Sprintf("=== shard %02d ===\n", i))
		sb.WriteString(getProperty)
		sb.WriteString("\n")
	}

	return sb.String(), nil
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (l *LeveldbShard) Compact(start []byte, limit []byte) error {
	for _, db := range l.dbs {
		err := db.CompactRange(util.Range{Start: start, Limit: limit})
		if err != nil {
			return err
		}
	}

	return nil
}

// Close all the DB
func (l *LeveldbShard) Close() error {
	for _, db := range l.dbs {
		err := db.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
