package local_cache

import (
	"bytes"
	"github.com/allegro/bigcache"
	"github.com/ethereum/go-ethereum/ethdb"
	"log"
	"sync"
	"time"
)

type cacheWrapper struct {
	*bigcache.BigCache
}

func (c *cacheWrapper) Put(key []byte, value []byte) error {
	return c.BigCache.Set(String(key), value)
}

func (c *cacheWrapper) Delete(key []byte) error {
	return c.BigCache.Delete(String(key))
}

type LocalCacheDatabase struct {
	lock sync.RWMutex

	enableReadCache bool

	remoteDB  ethdb.KeyValueStore
	deleteMap map[string]bool
	readCache *cacheWrapper
}

func NewLocalCacheDatabase(remoteDB ethdb.KeyValueStore) *LocalCacheDatabase {
	config := bigcache.DefaultConfig(10 * time.Minute)
	config.HardMaxCacheSize = 512
	config.MaxEntriesInWindow = 2000 * 10 * 60
	cache, _ := bigcache.NewBigCache(config)

	db := &LocalCacheDatabase{
		enableReadCache: true,

		remoteDB:  remoteDB,
		deleteMap: make(map[string]bool),
		readCache: &cacheWrapper{cache},
	}

	go func() {
		for range time.Tick(time.Second) {
			log.Printf("cache: %#v %d (%d)", cache.Stats(), cache.Len(), cache.Capacity())
		}
	}()

	return db
}

func (c *LocalCacheDatabase) Has(key []byte) (bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.remoteDB.Has(key)
}

func (c *LocalCacheDatabase) Get(key []byte) (ret []byte, err error) {
	if c.enableReadCache {
		if bytes.Compare(key, []byte("LastBlock")) != 0 {
			strKey := String(key)
			ret, err = c.readCache.Get(strKey)
			if err == nil {
				return ret, nil
			}

			defer func() {
				if err == nil {
					_ = c.readCache.Set(strKey, ret)
				}
			}()
		}
	}

	return c.remoteDB.Get(key)
}

func (c *LocalCacheDatabase) Put(key []byte, value []byte) error {
	if c.enableReadCache {
		_ = c.readCache.Put(key, value)
	}

	return c.remoteDB.Put(key, value)
}

func (c *LocalCacheDatabase) Delete(key []byte) error {
	if c.enableReadCache {
		_ = c.readCache.Delete(key)
	}

	return c.remoteDB.Delete(key)
}

func (c *LocalCacheDatabase) NewBatch() ethdb.Batch {
	return c.remoteDB.NewBatch()
}

func (c *LocalCacheDatabase) NewIterator() ethdb.Iterator {
	return c.remoteDB.NewIterator()
}

func (c *LocalCacheDatabase) NewIteratorWithStart(start []byte) ethdb.Iterator {
	return c.remoteDB.NewIteratorWithStart(start)
}

func (c *LocalCacheDatabase) NewIteratorWithPrefix(prefix []byte) ethdb.Iterator {
	return c.remoteDB.NewIteratorWithPrefix(prefix)
}

func (c *LocalCacheDatabase) Stat(property string) (string, error) {
	return c.remoteDB.Stat(property)
}

func (c *LocalCacheDatabase) Compact(start []byte, limit []byte) error {
	return c.remoteDB.Compact(start, limit)
}

func (c *LocalCacheDatabase) Close() error {
	return c.remoteDB.Close()
}
