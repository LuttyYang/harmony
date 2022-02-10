package leveldb_shard

import (
	"hash/crc32"
	"sync"
)

func mapDBIndex(key []byte, dbCount uint32) uint32 {
	return crc32.ChecksumIEEE(key) % dbCount
}

func parallelRunAndReturnErr(parallelNum int, cb func(index int) error) (err error) {
	wg := sync.WaitGroup{}

	for i := 0; i < parallelNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			runErr := cb(i)
			if runErr != nil {
				err = runErr
			}
		}(i)
	}

	wg.Wait()
	return err
}
