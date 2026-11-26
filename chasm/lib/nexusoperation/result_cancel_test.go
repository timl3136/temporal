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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var resultService = workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService

// operationMutableContext carries what a terminal operation transition reads for its metrics.
func operationMutableContext() *chasm.MockMutableContext {
	return &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return time.Now() },
		HandleNamespaceEntry: func() *namespace.Namespace {
			return namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0)
		},
		GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
			MetricTagConfig: dynamicconfig.GetTypedPropertyFn(NexusMetricTagConfig{}),
		}),
	}}
}

func systemEndpointRegistry(service, operation string) *chasm.Registry {
	reg := chasm.NewRegistry(log.NewNoopLogger())
	serviceProc := chasm.NewNexusServiceProcessor(service)
	serviceProc.MustRegisterOperation(operation, chasm.NewRegisterableNexusOperationProcessor(&testCancelProcessor{}))
	reg.NexusEndpointProcessor.MustRegisterServiceProcessor(serviceProc)
	return reg
}

// DEC-4: a started GetWorkflowExecutionResult operation records Canceled when the target acknowledges the cancel,
// because the target never reports a canceled outcome for a removed subscription. Other operations wait for the handler.
func TestCancellationAckCancelsResultOperation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		service        string
		operation      string
		cancelErr      error
		expectedOp     nexusoperationpb.OperationStatus
		expectedCancel nexusoperationpb.CancellationStatus
	}{
		{
			name:           "result operation is canceled on ack",
			service:        resultService.ServiceName,
			operation:      resultService.GetWorkflowExecutionResult.Name(),
			expectedOp:     nexusoperationpb.OPERATION_STATUS_CANCELED,
			expectedCancel: nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
		},
		{
			name:           "result operation stays open while the cancel retries",
			service:        resultService.ServiceName,
			operation:      resultService.GetWorkflowExecutionResult.Name(),
			cancelErr:      serviceerror.NewUnavailable("down"),
			expectedOp:     nexusoperationpb.OPERATION_STATUS_STARTED,
			expectedCancel: nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
		},
		{
			name:           "other operation waits for the handler outcome",
			service:        "service",
			operation:      "operation",
			expectedOp:     nexusoperationpb.OPERATION_STATUS_STARTED,
			expectedCancel: nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := &Operation{OperationState: &nexusoperationpb.OperationState{
				Status:         nexusoperationpb.OPERATION_STATUS_STARTED,
				Endpoint:       commonnexus.SystemEndpoint,
				Service:        tc.service,
				Operation:      tc.operation,
				ScheduledTime:  timestamppb.Now(),
				StartedTime:    timestamppb.Now(),
				RequestId:      "request-id",
				OperationToken: "subscription-token",
				Attempt:        1,
			}}
			cancellation := newCancellation(&nexusoperationpb.CancellationState{
				Status:  nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
				Attempt: 1,
			})
			env := newCancellationTaskTestEnv(t, op, cancellation, InvocationData{Input: mustToPayload(t, "test")},
				nexustest.FakeEndpointRegistry{}, nil, metrics.NoopMetricsHandler, time.Hour)
			target := historyservicemock.NewMockHistoryServiceClient(env.ctrl)
			target.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *historyservice.CancelNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CancelNexusOperationResponse, error) {
					require.Equal(t, "subscription-token", req.GetRequest().GetOperationToken())
					if tc.cancelErr != nil {
						return nil, tc.cancelErr
					}
					return &historyservice.CancelNexusOperationResponse{}, nil
				})
			env.handler.historyClient = target
			env.handler.config.NumHistoryShards = 4
			env.handler.chasmRegistry = systemEndpointRegistry(tc.service, tc.operation)
			env.setupReadComponent()
			env.mockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
					return nil, updateFn(operationMutableContext(), cancellation)
				})

			require.NoError(t, env.execute(&nexusoperationpb.CancellationTask{Attempt: 1}))
			require.Equal(t, tc.expectedCancel, cancellation.Status)
			require.Equal(t, tc.expectedOp, op.Status)
		})
	}
}

// DEC-2: a Started reply that arrives after the caller canceled the unstarted operation is rejected by the operation's
// own state check, independent of whether the operation is still in the tree.
func TestLateStartedReplyAfterCancelIsDropped(t *testing.T) {
	op := &Operation{OperationState: &nexusoperationpb.OperationState{
		Status:        nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		Endpoint:      commonnexus.SystemEndpoint,
		Service:       resultService.ServiceName,
		Operation:     resultService.GetWorkflowExecutionResult.Name(),
		ScheduledTime: timestamppb.Now(),
		RequestId:     "request-id",
		Attempt:       1,
	}}
	env := newInvocationTaskTestEnv(t, op, InvocationData{Input: mustToPayload(t, "test")},
		nexustest.FakeEndpointRegistry{}, nil, metrics.NoopMetricsHandler, time.Hour)
	target := historyservicemock.NewMockHistoryServiceClient(env.ctrl)
	target.EXPECT().StartNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *historyservice.StartNexusOperationRequest, ...grpc.CallOption) (*historyservice.StartNexusOperationResponse, error) {
			// The target attached; the caller's cancel commits before the reply arrives.
			require.NoError(t, TransitionCanceled.Apply(op, operationMutableContext(),
				EventCanceled{Failure: commonnexus.ResultSubscriptionCanceledFailure()}))
			return &historyservice.StartNexusOperationResponse{Response: &nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
					AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "late-token"},
				},
			}}, nil
		})
	env.handler.historyClient = target
	env.handler.config.NumHistoryShards = 4
	env.handler.config.MaxOperationTokenLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(1000)
	env.handler.chasmRegistry = systemEndpointRegistry(op.Service, op.Operation)
	env.setupReadComponent()
	env.setupUpdateComponent()

	require.ErrorIs(t, env.execute(&nexusoperationpb.InvocationTask{Attempt: 1}), chasm.ErrInvalidTransition)
	require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, op.Status)
	require.Empty(t, op.OperationToken)
}
