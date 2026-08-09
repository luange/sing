package udpnat

import (
	"context"
	"net/netip"
	"time"

	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/pipe"
	"github.com/sagernet/sing/contrab/freelru"
	"github.com/sagernet/sing/contrab/maphash"
)

type Service struct {
	cache      *freelru.Cache[netip.AddrPort, *natConn]
	handler    N.UDPConnectionHandlerEx
	prepare    PrepareFunc
	queueDepth int
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
	cache := common.Must1(freelru.New[netip.AddrPort, *natConn](options.Capacity, maphash.NewHasher[netip.AddrPort]().Hash32, options.Shared))
	cache.SetLifetime(timeout)
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
	conn, _, ok := s.cache.GetAndRefreshOrAdd(source.AddrPort(), func() (*natConn, bool) {
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
	select {
	case conn.packetChan <- packet:
	default:
		packet.Buffer.Release()
		N.PutPacketBuffer(packet)
	}
}

func (s *Service) Len() int {
	return s.cache.Len()
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
