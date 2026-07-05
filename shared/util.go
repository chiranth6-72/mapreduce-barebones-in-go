package shared

import "hash/fnv"

// Ihash hashes a key to a positive integer
func Ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
