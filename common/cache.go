package common

import (
	"bytes"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zcash-hackworks/lightwalletd/walletrpc"
)

type BlockCacheEntry struct {
	data []byte
	hash []byte
}

type BlockCache struct {
	MaxEntries int

	FirstBlock int
	LastBlock  int

	m map[int]*BlockCacheEntry

	mutex sync.RWMutex
}

func NewBlockCache(maxEntries int) *BlockCache {
	return &BlockCache{
		MaxEntries: maxEntries,
		FirstBlock: -1,
		LastBlock:  -1,
		m:          make(map[int]*BlockCacheEntry),
	}
}

func (c *BlockCache) Add(height int, block *walletrpc.CompactBlock) (error, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// If we already have this block or any higher blocks, a reorg
	// must have occurred; these must be re-added
	for i := height; i <= c.LastBlock; i++ {
		delete(c.m, i)
	}

	// Detect reorg, ingestor needs to handle it
	if c.m[height-1] != nil && !bytes.Equal(block.PrevHash, c.m[height-1].hash) {
		return nil, true
	}

	// Add the entry and update the counters
	data, err := proto.Marshal(block)
	if err != nil {
		println("Error marshalling block!")
		return err, false
	}

	c.m[height] = &BlockCacheEntry{
		data: data,
		hash: block.GetHash(),
	}

	c.LastBlock = height
	if c.FirstBlock < 0 || c.FirstBlock > height {
		c.FirstBlock = height
	}

	// remove any blocks that are older than the capacity of the cache
	for c.FirstBlock <= c.LastBlock-c.MaxEntries {
		delete(c.m, c.FirstBlock)
		c.FirstBlock++
	}

	return nil, false
}

func (c *BlockCache) Get(height int) *walletrpc.CompactBlock {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.m[height] == nil {
		return nil
	}

	serialized := &walletrpc.CompactBlock{}
	err := proto.Unmarshal(c.m[height].data, serialized)
	if err != nil {
		println("Error unmarshalling compact block")
		return nil
	}

	return serialized
}

func (c *BlockCache) GetLatestBlock() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.LastBlock
}
