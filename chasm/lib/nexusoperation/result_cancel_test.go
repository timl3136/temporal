package nexusoperation

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DEC-2: a Started reply that arrives after the caller canceled the unstarted operation finds the operation removed
// from the tree. The handler returns the tree's NotFound, which the queue drops as an invalid task instead of retrying.
func TestLateStartedReplyAfterCancelIsDropped(t *testing.T) {
	svc := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	newOp := func() *Operation {
		return &Operation{OperationState: &nexusoperationpb.OperationState{
			Status:        nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			Endpoint:      commonnexus.SystemEndpoint,
			Service:       svc.ServiceName,
			Operation:     svc.GetWorkflowExecutionResult.Name(),
			ScheduledTime: timestamppb.Now(),
			RequestId:     "request-id",
			Attempt:       1,
		}}
	}
	invocationData := InvocationData{Input: mustToPayload(t, "test")}
	// The env only supplies a configured task handler; the operation under test lives in the engine below.
	env := newInvocationTaskTestEnv(t, newOp(), invocationData, nexustest.FakeEndpointRegistry{}, nil, metrics.NoopMetricsHandler, time.Hour)

	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&mockStoreLibrary{}))
	require.NoError(t, registry.Register(&Library{}))
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithNamespaceName("ns-id", "ns-name"))
	storeRef := chasm.NewComponentRef[*mockStoreComponent](chasm.ExecutionKey{NamespaceID: "ns-id", BusinessID: "wf-id", RunID: "run-id"})
	op := newOp()
	_, err := engine.StartExecution(t.Context(), storeRef, func(ctx chasm.MutableContext) (chasm.RootComponent, error) {
		return &mockStoreComponent{invocationData: invocationData, Op: chasm.NewComponentField(ctx, op)}, nil
	})
	require.NoError(t, err)

	target := historyservicemock.NewMockHistoryServiceClient(env.ctrl)
	target.EXPECT().StartNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *historyservice.StartNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.StartNexusOperationResponse, error) {
			// The target attached; the caller's cancel commits before the reply arrives and removes the operation.
			_, err := engine.UpdateComponent(ctx, storeRef, func(_ chasm.MutableContext, c chasm.Component) error {
				c.(*mockStoreComponent).Op = chasm.NewEmptyField[*Operation]()
				return nil
			})
			require.NoError(t, err)
			return &historyservice.StartNexusOperationResponse{Response: &nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
					AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "late-token"},
				},
			}}, nil
		})
	env.handler.historyClient = target
	env.handler.config.NumHistoryShards = 4
	env.handler.config.MaxOperationTokenLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(1000)
	env.handler.chasmRegistry = chasm.NewRegistry(log.NewNoopLogger())
	serviceProc := chasm.NewNexusServiceProcessor(svc.ServiceName)
	serviceProc.MustRegisterOperation(svc.GetWorkflowExecutionResult.Name(), chasm.NewRegisterableNexusOperationProcessor(&testCancelProcessor{}))
	env.handler.chasmRegistry.NexusEndpointProcessor.MustRegisterServiceProcessor(serviceProc)

	dropped, err := chasmtest.ExecuteSideEffectTask(t.Context(), engine, op, chasm.SideEffectTaskHandler[*Operation, *nexusoperationpb.InvocationTask](env.handler),
		chasm.TaskAttributes{Destination: commonnexus.SystemEndpoint}, &nexusoperationpb.InvocationTask{Attempt: 1})
	require.False(t, dropped)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	require.NotErrorIs(t, err, chasm.ErrInvalidTransition)
	require.Empty(t, op.OperationToken, "the late reply is not recorded")
}
