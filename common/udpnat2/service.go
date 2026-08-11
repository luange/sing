package udpnat

import (
	"context"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/pipe"
)

type Service struct {
	cache              *flowTable
	handler            N.UDPConnectionHandlerEx
	prepare            PrepareFunc
	queueDepth         int
	newSessionRejected atomic.Uint64
	queueDrops         atomic.Uint64
}

// flowTable intentionally does not evict the least-recently-used active
// session. LRU eviction is unsafe for UDP: capacity pressure can close a live
// QUIC or DNS session. Entries are removed only when their idle deadline
// expires or the connection reports done; a full table rejects a new session.
// This mirrors DAE's endpoint-pool lifecycle and keeps memory bounded without
// interrupting established traffic.
type flowTable struct {
	shards   [64]flowShard
	capacity uint32
	count    atomic.Uint32
	lifetime time.Duration
	health   func(netip.AddrPort, *natConn) bool
	onRemove func(netip.AddrPort, *natConn)
}

type flowShard struct {
	sync.Mutex
	entries map[netip.AddrPort]flowEntry
}

type flowEntry struct {
	conn    *natConn
	expires time.Time
}

func newFlowTable(capacity uint32, lifetime time.Duration) *flowTable {
	if capacity == 0 {
		panic("invalid capacity")
	}
	table := &flowTable{capacity: capacity, lifetime: lifetime}
	return table
}

func (t *flowTable) shard(key netip.AddrPort) *flowShard {
	value := uint64(key.Port())
	address := key.Addr().As16()
	for _, b := range address {
		value = value*131 + uint64(b)
	}
	return &t.shards[value%uint64(len(t.shards))]
}

func (t *flowTable) SetHealthCheck(health func(netip.AddrPort, *natConn) bool) {
	t.health = health
}

func (t *flowTable) SetOnEvict(onRemove func(netip.AddrPort, *natConn)) {
	t.onRemove = onRemove
}

func (t *flowTable) Len() int { return int(t.count.Load()) }

// Add is retained for package-level tests and compatibility with callers that
// seed a table. Production packet admission uses GetAndRefreshOrAdd.
func (t *flowTable) Add(key netip.AddrPort, conn *natConn) bool {
	shard := t.shard(key)
	shard.Lock()
	defer shard.Unlock()
	if _, loaded := shard.entries[key]; loaded || !t.reserve() {
		return false
	}
	conn.cache = t
	conn.registered.Store(true)
	if shard.entries == nil {
		shard.entries = make(map[netip.AddrPort]flowEntry)
	}
	shard.entries[key] = flowEntry{conn: conn, expires: time.Now().Add(t.lifetime)}
	return true
}

func (t *flowTable) reserve() bool {
	for {
		current := t.count.Load()
		if current >= t.capacity {
			return false
		}
		if t.count.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (t *flowTable) release() { t.count.Add(^uint32(0)) }

func (t *flowTable) GetAndRefreshOrAdd(key netip.AddrPort, constructor func() (*natConn, bool)) (*natConn, bool, bool, bool) {
	shard := t.shard(key)
	shard.Lock()
	var stale *natConn
	if entry, loaded := shard.entries[key]; loaded {
		now := time.Now()
		if entry.expires.After(now) && t.health(key, entry.conn) {
			entry.expires = now.Add(t.lifetime)
			shard.entries[key] = entry
			shard.Unlock()
			return entry.conn, true, true, false
		}
		delete(shard.entries, key)
		t.release()
		stale = entry.conn
	}
	if !t.reserve() {
		shard.Unlock()
		if stale != nil && t.onRemove != nil {
			t.onRemove(key, stale)
		}
		return nil, false, false, true
	}
	// Keep this shard locked while constructing so the same source cannot
	// create two proxy endpoints concurrently.
	conn, ok := constructor()
	if !ok || conn == nil {
		t.release()
		shard.Unlock()
		if stale != nil && t.onRemove != nil {
			t.onRemove(key, stale)
		}
		return nil, false, false, false
	}
	if shard.entries == nil {
		shard.entries = make(map[netip.AddrPort]flowEntry)
	}
	shard.entries[key] = flowEntry{conn: conn, expires: time.Now().Add(t.lifetime)}
	conn.registered.Store(true)
	select {
	case <-conn.doneChan:
		delete(shard.entries, key)
		conn.registered.Store(false)
		t.release()
		shard.Unlock()
		if t.onRemove != nil {
			t.onRemove(key, conn)
		}
		return nil, false, false, false
	default:
	}
	shard.Unlock()
	if stale != nil && t.onRemove != nil {
		t.onRemove(key, stale)
	}
	return conn, false, true, false
}

func (t *flowTable) PeekWithLifetime(key netip.AddrPort) (*natConn, time.Time, bool) {
	shard := t.shard(key)
	shard.Lock()
	entry, loaded := shard.entries[key]
	shard.Unlock()
	if !loaded {
		return nil, time.Time{}, false
	}
	return entry.conn, entry.expires, true
}

func (t *flowTable) UpdateLifetime(key netip.AddrPort, conn *natConn, lifetime time.Duration) bool {
	shard := t.shard(key)
	shard.Lock()
	defer shard.Unlock()
	entry, loaded := shard.entries[key]
	if !loaded || entry.conn != conn {
		return false
	}
	entry.expires = time.Now().Add(lifetime)
	shard.entries[key] = entry
	return true
}

func (t *flowTable) Remove(key netip.AddrPort, conn *natConn) bool {
	shard := t.shard(key)
	shard.Lock()
	entry, loaded := shard.entries[key]
	if loaded && entry.conn == conn {
		delete(shard.entries, key)
		t.release()
	}
	shard.Unlock()
	// Remove is called by natConn.Close itself. Do not invoke onRemove here:
	// doing so would re-enter sync.Once in Close. Expiry/Purge paths invoke the
	// callback after removing the entry.
	return loaded && entry.conn == conn
}

func (t *flowTable) PurgeExpired() {
	now := time.Now()
	for index := range t.shards {
		shard := &t.shards[index]
		var removed []flowEntry
		shard.Lock()
		for key, entry := range shard.entries {
			if entry.expires.After(now) && t.health(key, entry.conn) {
				continue
			}
			delete(shard.entries, key)
			t.release()
			removed = append(removed, entry)
		}
		shard.Unlock()
		if t.onRemove != nil {
			for _, entry := range removed {
				t.onRemove(entry.conn.localAddr.AddrPort(), entry.conn)
			}
		}
	}
}

func (t *flowTable) Purge() {
	for index := range t.shards {
		shard := &t.shards[index]
		var removed []flowEntry
		shard.Lock()
		for _, entry := range shard.entries {
			removed = append(removed, entry)
		}
		shard.entries = nil
		shard.Unlock()
		if t.onRemove != nil {
			for _, entry := range removed {
				t.onRemove(entry.conn.localAddr.AddrPort(), entry.conn)
			}
		}
	}
}

type PrepareFunc func(source M.Socksaddr, destination M.Socksaddr, userData any) (bool, context.Context, N.PacketWriter, N.CloseHandlerFunc)

func New(handler N.UDPConnectionHandlerEx, prepare PrepareFunc, timeout time.Duration, shared bool) *Service {
	return NewWithOptions(handler, prepare, Options{
		Timeout:    timeout,
		Shared:     shared,
		Capacity:   1024,
		QueueDepth: 64,
	})
}

type Options struct {
	Timeout    time.Duration
	Shared     bool
	Capacity   uint32
	QueueDepth int
}

func NewWithOptions(handler N.UDPConnectionHandlerEx, prepare PrepareFunc, options Options) *Service {
	timeout := options.Timeout
	if timeout == 0 {
		panic("invalid timeout")
	}
	if options.Capacity <= 0 {
		panic("invalid capacity")
	}
	if options.QueueDepth <= 0 {
		panic("invalid queue depth")
	}
	cache := newFlowTable(options.Capacity, timeout)
	cache.SetHealthCheck(func(port netip.AddrPort, conn *natConn) bool {
		select {
		case <-conn.doneChan:
			return false
		default:
			return true
		}
	})
	cache.SetOnEvict(func(_ netip.AddrPort, conn *natConn) {
		conn.Close()
	})
	return &Service{
		cache:      cache,
		handler:    handler,
		prepare:    prepare,
		queueDepth: options.QueueDepth,
	}
}

func (s *Service) NewPacket(bufferSlices [][]byte, source M.Socksaddr, destination M.Socksaddr, userData any) {
	conn, _, ok, rejected := s.cache.GetAndRefreshOrAdd(source.AddrPort(), func() (*natConn, bool) {
		ok, ctx, writer, onClose := s.prepare(source, destination, userData)
		if !ok {
			return nil, false
		}
		newConn := &natConn{
			cache:        s.cache,
			writer:       writer,
			localAddr:    source,
			packetChan:   make(chan *N.PacketBuffer, s.queueDepth),
			doneChan:     make(chan struct{}),
			readDeadline: pipe.MakeDeadline(),
		}
		go s.handler.NewPacketConnectionEx(ctx, newConn, source, destination, onClose)
		return newConn, true
	})
	if !ok {
		if rejected {
			s.newSessionRejected.Add(1)
		}
		return
	}
	conn.handlerAccess.RLock()
	readWaitOptions := conn.readWaitOptions
	handler := conn.handler
	conn.handlerAccess.RUnlock()
	var dataLen int
	for _, bufferSlice := range bufferSlices {
		dataLen += len(bufferSlice)
	}
	buffer := readWaitOptions.NewBufferSize(dataLen)
	for _, bufferSlice := range bufferSlices {
		buffer.Write(bufferSlice)
	}
	readWaitOptions.PostReturn(buffer)
	if handler != nil {
		handler.NewPacketEx(buffer, destination)
		return
	}
	packet := N.NewPacketBuffer()
	*packet = N.PacketBuffer{
		Buffer:      buffer,
		Destination: destination,
	}
	if !conn.enqueue(packet) {
		s.queueDrops.Add(1)
		packet.Buffer.Release()
		N.PutPacketBuffer(packet)
	}
}

func (s *Service) Len() int {
	return s.cache.Len()
}

type RuntimeStats struct {
	Sessions           uint64
	NewSessionRejected uint64
	QueueDrops         uint64
}

func (s *Service) RuntimeStats() RuntimeStats {
	if s == nil {
		return RuntimeStats{}
	}
	return RuntimeStats{
		Sessions:           uint64(s.cache.Len()),
		NewSessionRejected: s.newSessionRejected.Load(),
		QueueDrops:         s.queueDrops.Load(),
	}
}

func (s *Service) NewPacketBatch(buffers []*buf.Buffer, sources []M.Socksaddr, destination M.Socksaddr, userData any) {
	if len(buffers) != len(sources) {
		buf.ReleaseMulti(buffers)
		return
	}
	for index, buffer := range buffers {
		s.NewPacket([][]byte{buffer.Bytes()}, sources[index], destination, userData)
		buffer.Release()
	}
}

func (s *Service) Purge() {
	s.cache.Purge()
}

func (s *Service) PurgeExpired() {
	s.cache.PurgeExpired()
}
