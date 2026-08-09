package udpnat

import (
	"net/netip"
	"testing"
	"time"
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
