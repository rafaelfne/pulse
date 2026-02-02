package partition

import (
	"testing"
)

func TestRouter(t *testing.T) {
	router := NewRouter(4)

	if router.NumPartitions() != 4 {
		t.Errorf("NumPartitions() = %d, want 4", router.NumPartitions())
	}

	// Test empty key always goes to partition 0
	p := router.Route("")
	if p != 0 {
		t.Errorf("Route(\"\") = %d, want 0", p)
	}

	// Test consistent routing for same key
	key := "test-key"
	p1 := router.Route(key)
	p2 := router.Route(key)
	if p1 != p2 {
		t.Errorf("Route() not consistent: %d != %d", p1, p2)
	}

	// Test partition bounds
	if p1 < 0 || p1 >= router.NumPartitions() {
		t.Errorf("Route() out of bounds: %d", p1)
	}
}

func TestRouterDistribution(t *testing.T) {
	router := NewRouter(4)
	counts := make([]int, 4)

	// Route many keys and check distribution
	for i := 0; i < 1000; i++ {
		key := string(rune(i))
		p := router.Route(key)
		if p < 0 || p >= 4 {
			t.Fatalf("Route() out of bounds: %d", p)
		}
		counts[p]++
	}

	// Each partition should have some keys (not checking perfect distribution)
	for i, count := range counts {
		if count == 0 {
			t.Errorf("Partition %d has no keys", i)
		}
	}
}

func TestNewRouterZeroPartitions(t *testing.T) {
	router := NewRouter(0)
	if router.NumPartitions() != 1 {
		t.Errorf("NewRouter(0) should default to 1 partition, got %d", router.NumPartitions())
	}
}

func TestNewRouterNegativePartitions(t *testing.T) {
	router := NewRouter(-5)
	if router.NumPartitions() != 1 {
		t.Errorf("NewRouter(-5) should default to 1 partition, got %d", router.NumPartitions())
	}
}
