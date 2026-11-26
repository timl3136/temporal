package workflow_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/nexusoperations" //nolint:staticcheck // Covers the legacy HSM caller.
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const resultCallerRunID = "abcdef01-2345-6789-abcd-ef0123456789"

// treeEnv resolves the node on every access, like the History environment does. A missing node is the error the real
// environment turns into ErrStaleReference, which drops the task.
type treeEnv struct {
	root *hsm.Node
	path []hsm.Key
}

func (e treeEnv) Access(_ context.Context, _ hsm.Ref, _ hsm.AccessType, accessor func(*hsm.Node) error) error {
	node, err := e.root.Child(e.path)
	if err != nil {
		return err
	}
	return accessor(node)
}

func (treeEnv) Now() time.Time { return time.Now() }

// resultCancelHarness drives the real command handlers and task executors for one caller operation against a
// scripted History target.
type resultCancelHarness struct {
	t      *testing.T
	tcx    testContext
	reg    *hsm.Registry
	target *historyservicemock.MockHistoryServiceClient
	opKey  hsm.Key
	ref    hsm.Ref
	// attached counts subscriptions the scripted target accepted, including ones whose reply was lost.
	attached int
}

func newResultCancelHarness(t *testing.T, service, operation string, input *commonpb.Payload) *resultCancelHarness {
	cfg := *defaultConfig
	cfg.MaxServiceNameLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(100)
	cfg.MaxOperationNameLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(100)
	tcx := newTestContext(t, &cfg)
	nsID := tests.GlobalNamespaceEntry.ID().String()
	tcx.execInfo.NamespaceId = nsID
	tcx.execInfo.WorkflowId = "caller-wf"
	tcx.execState.RunId = resultCallerRunID
	tcx.ms.EXPECT().HasAnyBufferedEvent(gomock.Any()).Return(false).AnyTimes()
	tcx.ms.EXPECT().LoadHistoryEvent(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, []byte) (*historypb.HistoryEvent, error) { return tcx.history.Events[0], nil }).AnyTimes()
	tcx.ms.EXPECT().Now().DoAndReturn(time.Now).AnyTimes()
	tcx.ms.EXPECT().GetWorkflowType().Return(&commonpb.WorkflowType{Name: "caller-type"}).AnyTimes()

	svc := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{
		dynamicconfig.EnableGetWorkflowExecutionResultFromWorkflow.Key(): true,
	}, log.NewNoopLogger())
	serviceProcessor := chasm.NewNexusServiceProcessor(svc.ServiceName)
	serviceProcessor.MustRegisterOperation(svc.GetWorkflowExecutionResult.Name(),
		chasmworkflow.NewGetWorkflowExecutionResultOperationProcessor(chasmworkflow.NewConfig(dc)))
	tcx.nexusProcessor.MustRegisterServiceProcessor(serviceProcessor)

	require.NoError(t, tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1000}, 1, &commandpb.Command{
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:  commonnexus.SystemEndpoint,
				Service:   service,
				Operation: operation,
				Input:     input,
			},
		},
	}))
	require.Len(t, tcx.history.Events, 1)

	ctrl := gomock.NewController(t)
	target := historyservicemock.NewMockHistoryServiceClient(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{
		Config: &nexusoperations.Config{
			RequestTimeout:                      dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Hour),
			MinRequestTimeout:                   dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Millisecond),
			PayloadSizeLimit:                    dynamicconfig.GetIntPropertyFnFilteredByNamespace(2 * 1024 * 1024),
			MaxOperationTokenLength:             dynamicconfig.GetIntPropertyFnFilteredByNamespace(1000),
			CallbackURLTemplate:                 dynamicconfig.GetStringPropertyFn("http://localhost/callback"),
			UseNewFailureWireFormat:             dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
			RecordCancelRequestCompletionEvents: dynamicconfig.GetBoolPropertyFn(true),
			NumHistoryShards:                    4,
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
			EnableOutboundTaskThrottleController: dynamicconfig.GetBoolPropertyFn(false),
		},
		CallbackTokenGenerator: commonnexus.NewCallbackTokenGenerator(),
		NamespaceRegistry:      namespaceRegistry,
		MetricsHandler:         metrics.NoopMetricsHandler,
		Logger:                 log.NewNoopLogger(),
		HistoryClient:          target,
		ChasmRegistry:          &chasm.Registry{NexusEndpointProcessor: tcx.nexusProcessor},
	}))
	return &resultCancelHarness{
		t:      t,
		tcx:    tcx,
		reg:    reg,
		target: target,
		opKey:  hsm.Key{Type: nexusoperations.OperationMachineType, ID: strconv.FormatInt(tcx.history.Events[0].EventId, 10)},
		ref: hsm.Ref{
			WorkflowKey:     definition.NewWorkflowKey(nsID, "caller-wf", resultCallerRunID),
			StateMachineRef: &persistencespb.StateMachineRef{},
		},
	}
}

func newResultHarness(t *testing.T) *resultCancelHarness {
	svc := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	input, err := sdkconverter.PreferProtoDataConverter.ToPayloads(&workflowservice.GetWorkflowExecutionResultRequest{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "target-wf"},
	})
	require.NoError(t, err)
	return newResultCancelHarness(t, svc.ServiceName, svc.GetWorkflowExecutionResult.Name(), input.Payloads[0])
}

func (h *resultCancelHarness) cancel() {
	h.t.Helper()
	require.NoError(h.t, h.tcx.cancelHandler(context.Background(), h.tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
		Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
			RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
				ScheduledEventId: h.tcx.history.Events[0].EventId,
			},
		},
	}))
}

func (h *resultCancelHarness) runStart() error {
	return h.reg.ExecuteImmediateTask(context.Background(), treeEnv{root: h.tcx.ms.HSM(), path: []hsm.Key{h.opKey}}, h.ref,
		nexusoperations.InvocationTask{EndpointName: commonnexus.SystemEndpoint})
}

func (h *resultCancelHarness) runCancel() error {
	return h.reg.ExecuteImmediateTask(context.Background(),
		treeEnv{root: h.tcx.ms.HSM(), path: []hsm.Key{h.opKey, nexusoperations.CancelationMachineKey}}, h.ref,
		nexusoperations.CancelationTask{EndpointName: commonnexus.SystemEndpoint})
}

func (h *resultCancelHarness) operation() (nexusoperations.Operation, error) {
	node, err := h.tcx.ms.HSM().Child([]hsm.Key{h.opKey})
	if err != nil {
		return nexusoperations.Operation{}, err
	}
	return hsm.MachineData[nexusoperations.Operation](node)
}

func (h *resultCancelHarness) eventTypes() []enumspb.EventType {
	var types []enumspb.EventType
	for _, e := range h.tcx.history.Events {
		types = append(types, e.EventType)
	}
	return types
}

// expectStart scripts the target's reply to one start attempt. inFlight runs after the target attached and before
// the reply reaches the caller.
func (h *resultCancelHarness) expectStart(reply func() (*historyservice.StartNexusOperationResponse, error), inFlight func()) {
	h.target.EXPECT().StartNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *historyservice.StartNexusOperationRequest, ...grpc.CallOption) (*historyservice.StartNexusOperationResponse, error) {
			h.attached++
			if inFlight != nil {
				inFlight()
			}
			return reply()
		})
}

func attachedReply(token string) func() (*historyservice.StartNexusOperationResponse, error) {
	return func() (*historyservice.StartNexusOperationResponse, error) {
		return &historyservice.StartNexusOperationResponse{Response: &nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
				AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: token},
			},
		}}, nil
	}
}

func lostReply() (*historyservice.StartNexusOperationResponse, error) {
	return nil, serviceerror.NewUnavailable("reply lost")
}

func requireCanceledByCaller(t *testing.T, event *historypb.HistoryEvent) {
	t.Helper()
	require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, event.EventType)
	require.NotNil(t, event.GetNexusOperationCanceledEventAttributes().GetFailure().GetCause().GetCanceledFailureInfo())
}

// DEC-2: an explicit cancel before the token is known settles the caller operation as Canceled at once and never sends
// a target cancel, whatever order the start attempt and its reply take.
func TestGetWorkflowExecutionResultCancelBeforeStarted(t *testing.T) {
	scheduledCanceled := []enumspb.EventType{
		enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
	}

	t.Run("cancel before attach", func(t *testing.T) {
		h := newResultHarness(t)
		h.cancel()
		require.Equal(t, scheduledCanceled, h.eventTypes())
		requireCanceledByCaller(t, h.tcx.history.Events[2])
		// The queued start attempt finds no operation: no start is sent and nothing attaches.
		require.ErrorIs(t, h.runStart(), hsm.ErrStateMachineNotFound)
		require.Zero(t, h.attached)
	})

	t.Run("cancel after attach but before Started", func(t *testing.T) {
		h := newResultHarness(t)
		h.expectStart(attachedReply("token"), h.cancel)
		// The late Started reply finds no operation and is dropped; no target cancel follows it.
		require.ErrorIs(t, h.runStart(), hsm.ErrStateMachineNotFound)
		require.Equal(t, scheduledCanceled, h.eventTypes())
		require.Equal(t, 1, h.attached, "the late attach stays on the target until it closes")
	})

	t.Run("retry delivered after cancel", func(t *testing.T) {
		h := newResultHarness(t)
		h.expectStart(lostReply, nil)
		require.NoError(t, h.runStart())
		op, err := h.operation()
		require.NoError(t, err)
		require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())

		h.cancel()
		require.Equal(t, scheduledCanceled, h.eventTypes())
		// The retry finds no operation: no second start and no target cancel.
		require.ErrorIs(t, h.runStart(), hsm.ErrStateMachineNotFound)
		require.Equal(t, 1, h.attached)
	})
}

// DEC-4: with a token the operation stays open until the target acknowledges the cancel, and the first terminal event
// wins.
func TestGetWorkflowExecutionResultCancelWhileStarted(t *testing.T) {
	started := func(t *testing.T) *resultCancelHarness {
		h := newResultHarness(t)
		h.expectStart(attachedReply("subscription-token"), nil)
		require.NoError(t, h.runStart())
		h.cancel()
		require.Equal(t, []enumspb.EventType{
			enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
		}, h.eventTypes())
		_, err := h.operation()
		require.NoError(t, err, "the operation stays open until the target acknowledges")
		return h
	}
	completeFromTarget := func(h *resultCancelHarness) error {
		event := h.tcx.ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
				NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
					ScheduledEventId: h.tcx.history.Events[0].EventId,
				},
			}
		})
		return nexusoperations.CompletedEventDefinition{}.Apply(h.tcx.ms.HSM(), event)
	}

	t.Run("target ack records Canceled", func(t *testing.T) {
		h := started(t)
		h.target.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, req *historyservice.CancelNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CancelNexusOperationResponse, error) {
				require.Equal(t, "subscription-token", req.GetRequest().GetOperationToken())
				return &historyservice.CancelNexusOperationResponse{}, nil
			})
		require.NoError(t, h.runCancel())
		require.Equal(t, []enumspb.EventType{
			enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		}, h.eventTypes())
		requireCanceledByCaller(t, h.tcx.history.Events[4])
		_, err := h.operation()
		require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
		// A result delivered after the ack finds no operation.
		require.ErrorIs(t, completeFromTarget(h), hsm.ErrStateMachineNotFound)
	})

	t.Run("completion before ack wins", func(t *testing.T) {
		h := started(t)
		require.NoError(t, completeFromTarget(h))
		// The pending cancel finds no operation and is never sent.
		require.ErrorIs(t, h.runCancel(), hsm.ErrStateMachineNotFound)
		require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, h.tcx.history.Events[len(h.tcx.history.Events)-1].EventType)
	})

	t.Run("retryable cancel failure keeps the operation open", func(t *testing.T) {
		h := started(t)
		h.target.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("down"))
		require.NoError(t, h.runCancel())
		op, err := h.operation()
		require.NoError(t, err)
		require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State())
		require.Len(t, h.tcx.history.Events, 3)
	})

	t.Run("timeout does not cancel the target", func(t *testing.T) {
		h := newResultHarness(t)
		h.expectStart(attachedReply("subscription-token"), nil)
		require.NoError(t, h.runStart())
		node, err := h.tcx.ms.HSM().Child([]hsm.Key{h.opKey})
		require.NoError(t, err)
		require.NoError(t, h.reg.ExecuteTimerTask(treeEnv{}, node, nexusoperations.ScheduleToCloseTimeoutTask{}))
		require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, h.tcx.history.Events[len(h.tcx.history.Events)-1].EventType)
		_, err = h.operation()
		require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
	})
}

// Other operations keep today's deferred cancel: the cancel waits for Started and is then sent to the handler.
func TestOtherOperationKeepsDeferredCancel(t *testing.T) {
	input, err := sdkconverter.PreferProtoDataConverter.ToPayloads(&workflowservice.GetWorkflowExecutionResultRequest{})
	require.NoError(t, err)
	h := newResultCancelHarness(t, "caller-info-test", "test", input.Payloads[0])
	h.cancel()
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
	}, h.eventTypes())

	h.expectStart(attachedReply("token"), nil)
	require.NoError(t, h.runStart())
	node, err := h.tcx.ms.HSM().Child([]hsm.Key{h.opKey, nexusoperations.CancelationMachineKey})
	require.NoError(t, err)
	cancelation, err := hsm.MachineData[nexusoperations.Cancelation](node)
	require.NoError(t, err)
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED, cancelation.State())

	h.target.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).Return(&historyservice.CancelNexusOperationResponse{}, nil)
	require.NoError(t, h.runCancel())
	op, err := h.operation()
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State(), "the handler reports the outcome")
}
