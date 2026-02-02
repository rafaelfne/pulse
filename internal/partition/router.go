package partition

import (
	"hash/fnv"
)

// Router determines which partition an event should go to.
type Router struct {
	numPartitions int
}

// NewRouter creates a new partition router with the given number of partitions.
func NewRouter(numPartitions int) *Router {
	if numPartitions <= 0 {
		numPartitions = 1
	}
	return &Router{
		numPartitions: numPartitions,
	}
}

// Route determines the partition ID for a given key using FNV-1a hash.
// If key is empty, returns partition 0.
func (r *Router) Route(key string) int {
	if key == "" {
		return 0
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % r.numPartitions
}

// NumPartitions returns the total number of partitions.
func (r *Router) NumPartitions() int {
	return r.numPartitions
}
