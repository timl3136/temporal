package nexusoperation

import (
	"context"
	"errors"
	commonpb "go.temporal.io/api/common/v1"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type callerInfoProcessor struct {
	caller *historyservice.NexusCallerInfo
}

func (p *callerInfoProcessor) ProcessInput(ctx chasm.NexusOperationProcessorContext, _ *emptypb.Empty) (*chasm.NexusOperationProcessorResult, error) {
	p.caller = ctx.Caller
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{NamespaceID: "target-ns", BusinessID: "target-wf"},
	}, nil
}

func newCallerInfoInvocationTest(t *testing.T) (*invocationSystem, *historyservicemock.MockHistoryServiceClient, *callerInfoProcessor, chasm.ExecutionKey) {
	t.Helper()
	processor := &callerInfoProcessor{}
	serviceProcessor := chasm.NewNexusServiceProcessor("caller-info-test")
	serviceProcessor.MustRegisterOperation("test", chasm.NewRegisterableNexusOperationProcessor(processor))
	endpointProcessor := chasm.NewNexusEndpointProcessor()
	endpointProcessor.MustRegisterServiceProcessor(serviceProcessor)
	ctrl := gomock.NewController(t)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	key := chasm.ExecutionKey{NamespaceID: "caller-ns", BusinessID: "caller-wf", RunID: "caller-run"}
	return &invocationSystem{
		ns:            namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Id: key.NamespaceID, Name: "caller-ns"}, nil, false, nil, 0),
		chasmRegistry: &chasm.Registry{NexusEndpointProcessor: endpointProcessor},
		historyClient: historyClient,
		config:        &Config{NumHistoryShards: 4},
	}, historyClient, processor, key
}

func newCallerInfoTestOperation(t *testing.T, store *mockStoreComponent) *Operation {
	t.Helper()
	input, err := sdk.PreferProtoDataConverter.ToPayloads(&emptypb.Empty{})
	require.NoError(t, err)
	store.invocationData = InvocationData{Input: input.Payloads[0]}
	op := NewOperation(&nexusoperationpb.OperationState{
		Service:       "caller-info-test",
		Operation:     "test",
		RequestId:     "scheduled-request-id",
		ScheduledTime: timestamppb.New(time.Now()),
	})
	op.Store = chasm.NewMockParentPtr[OperationStore](store)
	return op
}

func TestInvocationSystemStartPassesCallerInfo(t *testing.T) {
	system, historyClient, processor, key := newCallerInfoInvocationTest(t)
	op := newCallerInfoTestOperation(t, &mockStoreComponent{})
	ctx := &chasm.MockContext{HandleExecutionKey: func() chasm.ExecutionKey { return key }}
	args, err := op.loadStartArgs(ctx, chasm.NoValue(nil))
	require.NoError(t, err)
	expected := &historyservice.NexusCallerInfo{
		NamespaceId: key.NamespaceID,
		WorkflowId:  key.BusinessID,
		RunId:       key.RunID,
		RequestId:   "scheduled-request-id",
	}
	callErr := errors.New("stop after checking the request")
	historyClient.EXPECT().StartNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.StartNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.StartNexusOperationResponse, error) {
			require.Equal(t, expected, request.GetCallerInfo())
			return nil, callErr
		},
	)
	_, err = system.Start(t.Context(), args, nexus.StartOperationOptions{})
	require.ErrorIs(t, err, callErr)
	require.Equal(t, expected, processor.caller)
}

func TestLoadStartArgsStandaloneHasNoCallerInfo(t *testing.T) {
	op := NewOperation(&nexusoperationpb.OperationState{
		Service:       "caller-info-test",
		Operation:     "test",
		RequestId:     "standalone-request-id",
		ScheduledTime: timestamppb.New(time.Now()),
	})
	op.RequestData = chasm.NewDataField(nil, &nexusoperationpb.OperationRequestData{Input: &commonpb.Payload{}})
	key := chasm.ExecutionKey{NamespaceID: "ns", BusinessID: "op-id", RunID: "run"}
	ns := namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Id: key.NamespaceID, Name: "ns"}, nil, false, nil, 0)
	args, err := op.loadStartArgs(&chasm.MockContext{
		HandleExecutionKey:   func() chasm.ExecutionKey { return key },
		HandleNamespaceEntry: func() *namespace.Namespace { return ns },
	}, chasm.NoValue(nil))
	require.NoError(t, err)
	require.Nil(t, args.callerInfo)
}

func TestInvocationSystemCancelPassesCallerInfo(t *testing.T) {
	system, historyClient, processor, key := newCallerInfoInvocationTest(t)
	op := newCallerInfoTestOperation(t, &mockStoreComponent{})
	ctx := &chasm.MockContext{HandleExecutionKey: func() chasm.ExecutionKey { return key }}
	cancellation := &Cancellation{CancellationState: &nexusoperationpb.CancellationState{}}
	cancellation.Operation = chasm.NewMockParentPtr[*Operation](op)
	args, err := cancellation.loadArgs(ctx, chasm.NoValue(nil))
	require.NoError(t, err)
	expected := &historyservice.NexusCallerInfo{
		NamespaceId: key.NamespaceID,
		WorkflowId:  key.BusinessID,
		RunId:       key.RunID,
		RequestId:   "scheduled-request-id",
	}
	callErr := errors.New("stop after checking the request")
	historyClient.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.CancelNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CancelNexusOperationResponse, error) {
			require.Equal(t, expected, request.GetCallerInfo())
			return nil, callErr
		},
	)
	err = system.Cancel(t.Context(), args, nexus.CancelOperationOptions{})
	require.ErrorIs(t, err, callErr)
	require.Equal(t, expected, processor.caller)
}
