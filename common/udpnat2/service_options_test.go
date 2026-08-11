package udpnat

import (
	"net/netip"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestNewPreservesLegacyDefaults(t *testing.T) {
	service := New(nil, nil, time.Second, false)
	if service.queueDepth != 64 {
		t.Fatalf("legacy queue depth = %d, want 64", service.queueDepth)
	}
	for index := 0; index < 1025; index++ {
		service.cache.Add(netip.AddrPortFrom(netip.IPv4Unspecified(), uint16(index)), &natConn{doneChan: make(chan struct{})})
	}
	if service.Len() != 1024 {
		t.Fatalf("legacy capacity = %d, want 1024", service.Len())
	}
}

func TestNewWithOptionsAppliesCapacityAndQueueDepth(t *testing.T) {
	service := NewWithOptions(nil, nil, Options{
		Timeout:    time.Second,
		Capacity:   2,
		QueueDepth: 4,
	})
	if service.queueDepth != 4 {
		t.Fatalf("queue depth = %d, want 4", service.queueDepth)
	}
	for index := 0; index < 3; index++ {
		service.cache.Add(netip.AddrPortFrom(netip.IPv4Unspecified(), uint16(index)), &natConn{doneChan: make(chan struct{})})
	}
	if service.Len() != 2 {
		t.Fatalf("capacity = %d, want 2", service.Len())
	}
}

func TestCapacityNeverEvictsActiveSession(t *testing.T) {
	service := NewWithOptions(nil, nil, Options{Timeout: time.Minute, Capacity: 1, QueueDepth: 1})
	first := &natConn{doneChan: make(chan struct{})}
	key1 := netip.MustParseAddrPort("192.0.2.1:1000")
	key2 := netip.MustParseAddrPort("192.0.2.1:1001")
	if !service.cache.Add(key1, first) {
		t.Fatal("failed to seed first session")
	}
	second := &natConn{doneChan: make(chan struct{})}
	if service.cache.Add(key2, second) {
		t.Fatal("full table admitted a new session")
	}
	if service.Len() != 1 {
		t.Fatalf("table length = %d, want 1", service.Len())
	}
	select {
	case <-first.doneChan:
		t.Fatal("active session was closed under pressure")
	default:
	}
}

func TestConnectionCloseRemovesEntryImmediately(t *testing.T) {
	service := NewWithOptions(nil, nil, Options{Timeout: time.Minute, Capacity: 2, QueueDepth: 1})
	key := netip.MustParseAddrPort("192.0.2.2:1000")
	conn := &natConn{cache: service.cache, localAddr: M.SocksaddrFromNetIP(key), doneChan: make(chan struct{}), packetChan: make(chan *N.PacketBuffer, 1)}
	if !service.cache.Add(key, conn) {
		t.Fatal("failed to seed session")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if service.Len() != 0 {
		t.Fatalf("table length after close = %d, want 0", service.Len())
	}
}

func TestAdmissionPressureRemainsBounded(t *testing.T) {
	const capacity = 128
	service := NewWithOptions(nil, nil, Options{Timeout: time.Minute, Capacity: capacity, QueueDepth: 1})
	for index := 0; index < 10000; index++ {
		key := netip.AddrPortFrom(netip.MustParseAddr("198.51.100.1"), uint16(index))
		service.cache.Add(key, &natConn{doneChan: make(chan struct{})})
	}
	if got := service.Len(); got > capacity {
		t.Fatalf("table grew past admission budget: %d > %d", got, capacity)
	}
}

func TestNewWithOptionsRejectsInvalidBounds(t *testing.T) {
	for _, options := range []Options{
		{Capacity: 1, QueueDepth: 1},
		{Timeout: time.Second, QueueDepth: 1},
		{Timeout: time.Second, Capacity: 1},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid options did not panic")
				}
			}()
			NewWithOptions(nil, nil, options)
		}()
	}
}
