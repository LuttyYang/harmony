package leveldb_shard

import (
	"hash/crc32"
)

func mapDBIndex(key []byte, dbCount uint32) uint32 {
	return crc32.ChecksumIEEE(key) % dbCount
}
