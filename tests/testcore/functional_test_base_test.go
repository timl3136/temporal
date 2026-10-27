package testcore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/health"
	"go.temporal.io/server/service/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type FunctionalTestBaseSuite struct {
	functionalTestBase
}

func TestFunctionalTestBaseSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &FunctionalTestBaseSuite{})
}

func (s *FunctionalTestBaseSuite) SetupSuite() {
	s.SetupSuiteWithCluster()
}

func (s *FunctionalTestBaseSuite) TearDownSuite() {
	s.TearDownCluster()
}

func (s *FunctionalTestBaseSuite) SetupTest() {
	s.functionalTestBase.SetupTest()
}

// A peer can call grpc.health.v1 on history and matching over the same port as their API.
func (s *FunctionalTestBaseSuite) TestHistoryAndMatchingHealthPing() {
	for _, tc := range []struct{ addr, service string }{
		{s.GetTestCluster().HistoryGRPCAddress(), "temporal.api.workflowservice.v1.HistoryService"},
		{s.GetTestCluster().MatchingGRPCAddress(), "temporal.api.workflowservice.v1.MatchingService"},
	} {
		conn, err := grpc.NewClient(tc.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		s.NoError(err, tc.service)
		s.NoError(health.PingGRPCHealth(context.Background(), conn, "", time.Second), tc.service)
		s.Eventually(
			func() bool {
				resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: tc.service})
				return err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
			},
			20*time.Second,
			100*time.Millisecond,
			tc.service,
		)
		_ = conn.Close()
	}
}

func (s *FunctionalTestBaseSuite) TestWorkerServiceHealthCheck() {
	// This test verifies that the worker service exposes a working gRPC health check endpoint.
	conn, err := grpc.NewClient(
		s.WorkerGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.NoError(err)
	defer func() { _ = conn.Close() }()

	healthClient := healthpb.NewHealthClient(conn)
	s.Eventually(
		func() bool {
			resp, err := healthClient.Check(context.Background(), &healthpb.HealthCheckRequest{
				Service: worker.ServiceName,
			})
			return err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
		},
		10*time.Second,
		100*time.Millisecond,
	)
}
