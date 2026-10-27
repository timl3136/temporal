package health

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// hungHealthServer never answers, like a peer behind a dead connection.
type hungHealthServer struct {
	healthpb.UnimplementedHealthServer
}

func (hungHealthServer) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func startHealthServer(t *testing.T, register func(*grpc.Server)) (*grpc.Server, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return srv, lis.Addr().String()
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestPingGRPCHealth(t *testing.T) {
	const svc = "test.Service"
	hs := health.NewServer()
	hs.SetServingStatus(svc, healthpb.HealthCheckResponse_NOT_SERVING)
	_, healthyAddr := startHealthServer(t, func(s *grpc.Server) { healthpb.RegisterHealthServer(s, hs) })
	_, hungAddr := startHealthServer(t, func(s *grpc.Server) { healthpb.RegisterHealthServer(s, hungHealthServer{}) })
	stoppedSrv, stoppedAddr := startHealthServer(t, func(s *grpc.Server) { healthpb.RegisterHealthServer(s, health.NewServer()) })
	stoppedConn := dial(t, stoppedAddr)
	require.NoError(t, PingGRPCHealth(context.Background(), stoppedConn, "", time.Second))
	stoppedSrv.Stop() // connection was healthy, then the peer disappears

	t.Run("serving", func(t *testing.T) {
		require.NoError(t, PingGRPCHealth(context.Background(), dial(t, healthyAddr), "", time.Second))
	})
	t.Run("not serving is still reachable", func(t *testing.T) {
		require.NoError(t, PingGRPCHealth(context.Background(), dial(t, healthyAddr), svc, time.Second))
	})
	t.Run("unknown service is still reachable", func(t *testing.T) {
		require.NoError(t, PingGRPCHealth(context.Background(), dial(t, healthyAddr), "nope", time.Second))
	})
	t.Run("hung peer", func(t *testing.T) {
		err := PingGRPCHealth(context.Background(), dial(t, hungAddr), "", 200*time.Millisecond)
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	})
	t.Run("peer gone", func(t *testing.T) {
		err := PingGRPCHealth(context.Background(), stoppedConn, "", time.Second)
		require.Equal(t, codes.Unavailable, status.Code(err))
	})
}
