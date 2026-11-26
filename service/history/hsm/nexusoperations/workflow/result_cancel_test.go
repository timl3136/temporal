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
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const resultCallerRunID = "abcdef01-2345-6789-abcd-ef0123456789"

var resultService = workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService

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

type replayRoot struct{}

func (replayRoot) IsWorkflowExecutionRunning() bool { return true }
func (replayRoot) IsTransitionHistoryEnabled() bool { return false }

// resultCancelHarness drives the real command handlers and task executors for caller operations against a scripted
// History target. Events go through a real history builder, and commit closes a transaction the way mutable state
// does, so buffering and the emitted batches match production.
type resultCancelHarness struct {
	t              *testing.T
	tcx            testContext
	reg            *hsm.Registry
	target         *historyservicemock.MockHistoryServiceClient
	flag           *dynamicconfig.MemoryClient
	ref            hsm.Ref
	executorMetric *metricstest.Capture
	batches        [][]*historypb.HistoryEvent
	scheduled      []int64
	// attached counts subscriptions the scripted target accepted, including ones whose reply was lost.
	attached int
}

func newResultCancelHarness(t *testing.T) *resultCancelHarness {
	cfg := *defaultConfig
	cfg.MaxServiceNameLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(100)
	cfg.MaxOperationNameLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(100)
	tcx := newTestContext(t, &cfg)
	nsID := tests.GlobalNamespaceEntry.ID().String()
	tcx.execInfo.NamespaceId = nsID
	tcx.execInfo.WorkflowId = "caller-wf"
	tcx.execState.RunId = resultCallerRunID
	h := &resultCancelHarness{t: t, tcx: tcx, flag: dynamicconfig.NewMemoryClient()}
	h.newBuilder(5, nil)
	tcx.ms.EXPECT().HasAnyBufferedEvent(gomock.Any()).DoAndReturn(func(filter historybuilder.BufferedEventFilter) bool {
		return tcx.builder.b.HasAnyBufferedEvent(filter)
	}).AnyTimes()
	tcx.ms.EXPECT().LoadHistoryEvent(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, token []byte) (*historypb.HistoryEvent, error) {
		ref := &tokenspb.HistoryEventRef{}
		if err := proto.Unmarshal(token, ref); err != nil {
			return nil, err
		}
		for _, e := range tcx.history.Events {
			if e.EventId == ref.EventId {
				return e, nil
			}
		}
		return nil, serviceerror.NewNotFound("event not found")
	}).AnyTimes()
	tcx.ms.EXPECT().Now().DoAndReturn(time.Now).AnyTimes()
	tcx.ms.EXPECT().GetWorkflowType().Return(&commonpb.WorkflowType{Name: "caller-type"}).AnyTimes()

	h.flag.OverrideSetting(dynamicconfig.EnableGetWorkflowExecutionResultFromWorkflow, true)
	serviceProcessor := chasm.NewNexusServiceProcessor(resultService.ServiceName)
	serviceProcessor.MustRegisterOperation(resultService.GetWorkflowExecutionResult.Name(),
		chasmworkflow.NewGetWorkflowExecutionResultOperationProcessor(chasmworkflow.NewConfig(
			dynamicconfig.NewCollection(h.flag, log.NewNoopLogger()))))
	tcx.nexusProcessor.MustRegisterServiceProcessor(serviceProcessor)

	ctrl := gomock.NewController(t)
	h.target = historyservicemock.NewMockHistoryServiceClient(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	metricsHandler := metricstest.NewCaptureHandler()
	h.executorMetric = metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(h.executorMetric) })
	h.reg = hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(h.reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(h.reg))
	require.NoError(t, nexusoperations.RegisterEventDefinitions(h.reg))
	require.NoError(t, nexusoperations.RegisterExecutor(h.reg, nexusoperations.TaskExecutorOptions{
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
		MetricsHandler:         metricsHandler,
		Logger:                 log.NewNoopLogger(),
		HistoryClient:          h.target,
		ChasmRegistry:          &chasm.Registry{NexusEndpointProcessor: tcx.nexusProcessor},
	}))
	h.ref = hsm.Ref{
		WorkflowKey:     definition.NewWorkflowKey(nsID, "caller-wf", resultCallerRunID),
		StateMachineRef: &persistencespb.StateMachineRef{},
	}
	return h
}

func (h *resultCancelHarness) newBuilder(nextEventID int64, dbBuffered []*historypb.HistoryEvent) {
	var taskID int64
	h.tcx.builder.b = historybuilder.New(clock.NewRealTimeSource(), func(n int) ([]int64, error) {
		ids := make([]int64, n)
		for i := range ids {
			taskID++
			ids[i] = taskID
		}
		return ids, nil
	}, 1, nextEventID, dbBuffered, metrics.NoopMetricsHandler, dynamicconfig.GetIntPropertyFn(0))
}

// commit closes the current transaction. With a workflow task in flight, buffered events stay buffered for the next
// transaction; otherwise they are flushed into the emitted batch.
func (h *resultCancelHarness) commit(workflowTaskInFlight bool) {
	h.t.Helper()
	b := h.tcx.builder.b
	mutation, err := b.Finish(!workflowTaskInFlight)
	require.NoError(h.t, err)
	h.batches = append(h.batches, mutation.DBEventsBatches...)
	h.newBuilder(b.NextEventID(), mutation.MemBufferBatch)
}

func resultInput(t *testing.T) *commonpb.Payload {
	input, err := sdkconverter.PreferProtoDataConverter.ToPayloads(&workflowservice.GetWorkflowExecutionResultRequest{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "target-wf"},
	})
	require.NoError(t, err)
	return input.Payloads[0]
}

func (h *resultCancelHarness) schedule(service, operation string) (int64, error) {
	err := h.tcx.scheduleHandler(context.Background(), h.tcx.ms, commandValidator{maxPayloadSize: 1000}, 1, &commandpb.Command{
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:  commonnexus.SystemEndpoint,
				Service:   service,
				Operation: operation,
				Input:     resultInput(h.t),
			},
		},
	})
	if err != nil {
		return 0, err
	}
	id := h.tcx.history.Events[len(h.tcx.history.Events)-1].EventId
	h.scheduled = append(h.scheduled, id)
	h.commit(false)
	return id, nil
}

func (h *resultCancelHarness) scheduleResultWait() int64 {
	h.t.Helper()
	id, err := h.schedule(resultService.ServiceName, resultService.GetWorkflowExecutionResult.Name())
	require.NoError(h.t, err)
	return id
}

func opKey(id int64) hsm.Key {
	return hsm.Key{Type: nexusoperations.OperationMachineType, ID: strconv.FormatInt(id, 10)}
}

// cancel handles one RequestCancelNexusOperation command without closing the transaction.
func (h *resultCancelHarness) cancel(id int64) error {
	return h.tcx.cancelHandler(context.Background(), h.tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
		Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
			RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
				ScheduledEventId: id,
			},
		},
	})
}

// cancelAndCommit is a workflow task that cancels one operation.
func (h *resultCancelHarness) cancelAndCommit(id int64) {
	h.t.Helper()
	require.NoError(h.t, h.cancel(id))
	h.commit(false)
}

func (h *resultCancelHarness) runStart(id int64) error {
	err := h.reg.ExecuteImmediateTask(context.Background(), treeEnv{root: h.tcx.ms.HSM(), path: []hsm.Key{opKey(id)}}, h.ref,
		nexusoperations.InvocationTask{EndpointName: commonnexus.SystemEndpoint})
	if err == nil {
		h.commit(false)
	}
	return err
}

func (h *resultCancelHarness) runCancel(id int64) error {
	err := h.reg.ExecuteImmediateTask(context.Background(),
		treeEnv{root: h.tcx.ms.HSM(), path: []hsm.Key{opKey(id), nexusoperations.CancelationMachineKey}}, h.ref,
		nexusoperations.CancelationTask{EndpointName: commonnexus.SystemEndpoint})
	if err == nil {
		h.commit(false)
	}
	return err
}

// completeFromTarget applies a result delivered by the target's callback.
func (h *resultCancelHarness) completeFromTarget(id int64) error {
	event := h.tcx.ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
			NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{ScheduledEventId: id},
		}
	})
	return nexusoperations.CompletedEventDefinition{}.Apply(h.tcx.ms.HSM(), event)
}

func (h *resultCancelHarness) operation(id int64) (nexusoperations.Operation, error) {
	node, err := h.tcx.ms.HSM().Child([]hsm.Key{opKey(id)})
	if err != nil {
		return nexusoperations.Operation{}, err
	}
	return hsm.MachineData[nexusoperations.Operation](node)
}

// emitted returns the committed event types, in order.
func (h *resultCancelHarness) emitted() []enumspb.EventType {
	var types []enumspb.EventType
	for _, batch := range h.batches {
		for _, e := range batch {
			types = append(types, e.EventType)
		}
	}
	return types
}

// requireReplayMatches applies the emitted batches to a fresh tree, as a standby or reset does, and compares the
// resulting operations with the live tree.
func (h *resultCancelHarness) requireReplayMatches() {
	h.t.Helper()
	root, err := hsm.NewRoot(h.reg, workflow.StateMachineType, replayRoot{}, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(h.t, err)
	for _, batch := range h.batches {
		for _, e := range batch {
			def, ok := h.reg.EventDefinition(e.EventType)
			require.True(h.t, ok, "no definition for %v", e.EventType)
			require.NoError(h.t, def.Apply(root, e), "replaying %v", e.EventType)
		}
	}
	for _, id := range h.scheduled {
		live, liveErr := h.operation(id)
		replayedNode, replayErr := root.Child([]hsm.Key{opKey(id)})
		require.Equal(h.t, liveErr == nil, replayErr == nil, "operation %d presence", id)
		if liveErr == nil {
			replayed, err := hsm.MachineData[nexusoperations.Operation](replayedNode)
			require.NoError(h.t, err)
			require.Equal(h.t, live.State(), replayed.State(), "operation %d state", id)
		}
	}
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

func (h *resultCancelHarness) expectCancel(token string, err error) {
	h.target.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *historyservice.CancelNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CancelNexusOperationResponse, error) {
			require.Equal(h.t, token, req.GetRequest().GetOperationToken())
			if err != nil {
				return nil, err
			}
			return &historyservice.CancelNexusOperationResponse{}, nil
		})
}

// started schedules a result wait and starts it with the given token.
func (h *resultCancelHarness) started(token string) int64 {
	h.t.Helper()
	id := h.scheduleResultWait()
	h.expectStart(attachedReply(token), nil)
	require.NoError(h.t, h.runStart(id))
	return id
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

func requireCanceledByCaller(t *testing.T, events []*historypb.HistoryEvent) {
	t.Helper()
	var canceled []*historypb.HistoryEvent
	for _, e := range events {
		if e.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED {
			canceled = append(canceled, e)
		}
	}
	require.Len(t, canceled, 1)
	require.NotNil(t, canceled[0].GetNexusOperationCanceledEventAttributes().GetFailure().GetCause().GetCanceledFailureInfo())
}

func cancelCount(capture *metricstest.Capture) int {
	return len(capture.Snapshot()[chasmnexus.NexusOperationCancelCount.Name()])
}

var scheduledCanceled = []enumspb.EventType{
	enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
	enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
}

// DEC-2: an explicit cancel before the token is known settles the caller operation as Canceled at once and never sends
// a target cancel, whatever order the start attempt and its reply take.
func TestGetWorkflowExecutionResultCancelBeforeStarted(t *testing.T) {
	t.Run("cancel before attach", func(t *testing.T) {
		h := newResultCancelHarness(t)
		id := h.scheduleResultWait()
		h.cancelAndCommit(id)
		require.Equal(t, scheduledCanceled, h.emitted())
		requireCanceledByCaller(t, h.tcx.history.Events)
		require.Equal(t, 1, cancelCount(h.tcx.metricsCapture))
		// The queued start attempt finds no operation: no start is sent and nothing attaches.
		require.ErrorIs(t, h.runStart(id), hsm.ErrStateMachineNotFound)
		require.Zero(t, h.attached)
		h.requireReplayMatches()
	})

	t.Run("cancel after attach but before Started", func(t *testing.T) {
		h := newResultCancelHarness(t)
		id := h.scheduleResultWait()
		h.expectStart(attachedReply("token"), func() { h.cancelAndCommit(id) })
		// The late Started reply finds no operation and is dropped; no target cancel follows it.
		require.ErrorIs(t, h.runStart(id), hsm.ErrStateMachineNotFound)
		require.Equal(t, scheduledCanceled, h.emitted())
		require.Equal(t, 1, h.attached, "the late attach stays on the target until it closes")
		h.requireReplayMatches()
	})

	t.Run("retry delivered after cancel", func(t *testing.T) {
		h := newResultCancelHarness(t)
		id := h.scheduleResultWait()
		h.expectStart(lostReply, nil)
		require.NoError(t, h.runStart(id))
		op, err := h.operation(id)
		require.NoError(t, err)
		require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())

		h.cancelAndCommit(id)
		require.Equal(t, scheduledCanceled, h.emitted())
		// The retry finds no operation: no second start and no target cancel.
		require.ErrorIs(t, h.runStart(id), hsm.ErrStateMachineNotFound)
		require.Equal(t, 1, h.attached)
		h.requireReplayMatches()
	})

	t.Run("second cancel in the same workflow task fails the task", func(t *testing.T) {
		h := newResultCancelHarness(t)
		id := h.scheduleResultWait()
		require.NoError(t, h.cancel(id))
		var failWFT chasmworkflow.FailWorkflowTaskError
		require.ErrorAs(t, h.cancel(id), &failWFT)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFT.Cause)
		h.commit(false)
		require.Equal(t, scheduledCanceled, h.emitted())
		h.requireReplayMatches()
	})
}

// DEC-4: with a token the operation stays open until the target acknowledges the cancel, and the first terminal event
// wins.
func TestGetWorkflowExecutionResultCancelWhileStarted(t *testing.T) {
	startedCancelRequested := []enumspb.EventType{
		enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
	}
	cancelRequested := func(t *testing.T) (*resultCancelHarness, int64) {
		h := newResultCancelHarness(t)
		id := h.started("subscription-token")
		h.cancelAndCommit(id)
		require.Equal(t, startedCancelRequested, h.emitted())
		_, err := h.operation(id)
		require.NoError(t, err, "the operation stays open until the target acknowledges")
		require.Zero(t, cancelCount(h.tcx.metricsCapture))
		return h, id
	}

	t.Run("target ack records Canceled", func(t *testing.T) {
		h, id := cancelRequested(t)
		h.expectCancel("subscription-token", nil)
		require.NoError(t, h.runCancel(id))
		require.Equal(t, append(startedCancelRequested,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		), h.emitted())
		requireCanceledByCaller(t, h.tcx.history.Events)
		require.Equal(t, 1, cancelCount(h.executorMetric))
		_, err := h.operation(id)
		require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
		h.requireReplayMatches()
		// A result delivered after the ack finds no operation.
		require.ErrorIs(t, h.completeFromTarget(id), hsm.ErrStateMachineNotFound)
	})

	t.Run("completion before ack wins", func(t *testing.T) {
		h, id := cancelRequested(t)
		require.NoError(t, h.completeFromTarget(id))
		h.commit(false)
		// The pending cancel finds no operation and is never sent.
		require.ErrorIs(t, h.runCancel(id), hsm.ErrStateMachineNotFound)
		require.Equal(t, append(startedCancelRequested, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED), h.emitted())
		h.requireReplayMatches()
	})

	t.Run("retryable cancel failure keeps the operation open", func(t *testing.T) {
		h, id := cancelRequested(t)
		h.expectCancel("subscription-token", serviceerror.NewUnavailable("down"))
		require.NoError(t, h.runCancel(id))
		op, err := h.operation(id)
		require.NoError(t, err)
		require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State())
		require.Equal(t, startedCancelRequested, h.emitted())
	})

	// DEC-5: the caller keeps waiting when the target rejects the cancel.
	t.Run("non-retryable cancel failure keeps waiting", func(t *testing.T) {
		h, id := cancelRequested(t)
		h.expectCancel("subscription-token", serviceerror.NewInvalidArgument("bad token"))
		require.NoError(t, h.runCancel(id))
		op, err := h.operation(id)
		require.NoError(t, err)
		require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State())
		node, err := h.tcx.ms.HSM().Child([]hsm.Key{opKey(id), nexusoperations.CancelationMachineKey})
		require.NoError(t, err)
		cancelation, err := hsm.MachineData[nexusoperations.Cancelation](node)
		require.NoError(t, err)
		require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, cancelation.State())
		require.Equal(t, append(startedCancelRequested, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED), h.emitted())
		h.requireReplayMatches()
	})

	t.Run("cancel racing a buffered completion", func(t *testing.T) {
		h := newResultCancelHarness(t)
		id := h.started("subscription-token")
		// The result arrives while the workflow task that cancels is in flight.
		require.NoError(t, h.completeFromTarget(id))
		h.commit(true)
		h.cancelAndCommit(id)
		require.Equal(t, []enumspb.EventType{
			enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		}, h.emitted())
		h.requireReplayMatches()
	})

	t.Run("second waiter still receives its result", func(t *testing.T) {
		h := newResultCancelHarness(t)
		first := h.started("first-token")
		second := h.started("second-token")
		h.cancelAndCommit(first)
		h.expectCancel("first-token", nil)
		require.NoError(t, h.runCancel(first))
		require.NoError(t, h.completeFromTarget(second))
		h.commit(false)
		requireCanceledByCaller(t, h.tcx.history.Events)
		last := h.tcx.history.Events[len(h.tcx.history.Events)-1]
		require.Equal(t, second, last.GetNexusOperationCompletedEventAttributes().GetScheduledEventId())
		h.requireReplayMatches()
	})

	t.Run("cancel still routes after the flag is disabled", func(t *testing.T) {
		h := newResultCancelHarness(t)
		id := h.started("subscription-token")
		h.flag.OverrideSetting(dynamicconfig.EnableGetWorkflowExecutionResultFromWorkflow, false)
		_, err := h.schedule(resultService.ServiceName, resultService.GetWorkflowExecutionResult.Name())
		var failWFT chasmworkflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFT, "the flag is off for new waits")
		h.cancelAndCommit(id)
		h.expectCancel("subscription-token", nil)
		require.NoError(t, h.runCancel(id))
		requireCanceledByCaller(t, h.tcx.history.Events)
		h.requireReplayMatches()
	})

	t.Run("timeout does not cancel the target", func(t *testing.T) {
		h := newResultCancelHarness(t)
		id := h.started("subscription-token")
		node, err := h.tcx.ms.HSM().Child([]hsm.Key{opKey(id)})
		require.NoError(t, err)
		require.NoError(t, h.reg.ExecuteTimerTask(treeEnv{}, node, nexusoperations.ScheduleToCloseTimeoutTask{}))
		h.commit(false)
		require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, h.emitted()[len(h.emitted())-1])
		_, err = h.operation(id)
		require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
	})
}

// Other operations keep today's deferred cancel: the cancel waits for Started, is then sent to the handler, and the
// handler reports the outcome. A non-retryable History error is still retried for them.
func TestOtherOperationKeepsDeferredCancel(t *testing.T) {
	h := newResultCancelHarness(t)
	id, err := h.schedule("caller-info-test", "test")
	require.NoError(t, err)
	h.cancelAndCommit(id)
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
	}, h.emitted())

	h.expectStart(attachedReply("token"), nil)
	require.NoError(t, h.runStart(id))
	node, err := h.tcx.ms.HSM().Child([]hsm.Key{opKey(id), nexusoperations.CancelationMachineKey})
	require.NoError(t, err)
	cancelation, err := hsm.MachineData[nexusoperations.Cancelation](node)
	require.NoError(t, err)
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED, cancelation.State())

	h.expectCancel("token", serviceerror.NewInvalidArgument("bad token"))
	require.NoError(t, h.runCancel(id))
	cancelation, err = hsm.MachineData[nexusoperations.Cancelation](node)
	require.NoError(t, err)
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF, cancelation.State())

	h.expectCancel("token", nil)
	require.NoError(t, h.reg.ExecuteTimerTask(treeEnv{}, node, nexusoperations.CancelationBackoffTask{}))
	require.NoError(t, h.runCancel(id))
	op, err := h.operation(id)
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State(), "the handler reports the outcome")
	require.Zero(t, cancelCount(h.executorMetric))
	h.requireReplayMatches()
}
