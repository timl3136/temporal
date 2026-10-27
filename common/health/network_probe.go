package health

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// PingGRPCHealth calls grpc.health.v1 Check on conn and returns an error only if the network path
// failed (deadline exceeded or unavailable). Any other reply, including NOT_SERVING or NotFound,
// proves the peer was reachable, so it is not a connectivity failure.
//
// conn should be the connection real traffic uses for that host. A freshly dialed one can succeed
// while a pooled connection is stuck.
func PingGRPCHealth(
	ctx context.Context,
	conn grpc.ClientConnInterface,
	service string,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Unavailable:
		return err
	default:
		return nil
	}
}
