package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/types/known/emptypb"
)

var resultSvc = workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService

// resultTree is a real CHASM tree with a Workflow root whose history events go through a real history builder.
type resultTree struct {
	t        *testing.T
	root     *chasm.Node
	wfreg    *Registry
	builder  *historybuilder.HistoryBuilder
	executed []*historypb.HistoryEvent
	batches  [][]*historypb.HistoryEvent
}

func newResultTree(t *testing.T) *resultTree {
	svc := resultSvc
	cfg := *defaultConfig
	cfg.MaxServiceNameLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(100)
	cfg.MaxOperationNameLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(100)
	nexusProcessor := chasm.NewNexusEndpointProcessor()
	resultProcessor := chasm.NewNexusServiceProcessor(svc.ServiceName)
	resultProcessor.MustRegisterOperation(svc.GetWorkflowExecutionResult.Name(),
		NewGetWorkflowExecutionResultOperationProcessor(newResultProcessor(true).config))
	nexusProcessor.MustRegisterServiceProcessor(resultProcessor)
	otherProcessor := chasm.NewNexusServiceProcessor("other-service")
	otherProcessor.MustRegisterOperation("op", chasm.NewRegisterableNexusOperationProcessor(&callerInfoCapture{}))
	nexusProcessor.MustRegisterServiceProcessor(otherProcessor)

	logger := log.NewTestLogger()
	wfreg := NewRegistry()
	require.NoError(t, wfreg.Register(newNexusLibrary(&cfg, nexusProcessor, logger)))
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewLibrary(wfreg)))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	require.NoError(t, registry.Register(nexusoperation.NewNilLibrary()))

	rt := &resultTree{t: t, wfreg: wfreg}
	rt.newBuilder(5, nil)
	execInfo := &persistencespb.WorkflowExecutionInfo{NamespaceId: tests.GlobalNamespaceEntry.ID().String(), WorkflowId: callerWorkflowID}
	transitions := int64(1)
	backend := &chasm.MockNodeBackend{
		HandleGetExecutionInfo:  func() *persistencespb.WorkflowExecutionInfo { return execInfo },
		HandleGetNamespaceEntry: func() *namespace.Namespace { return tests.GlobalNamespaceEntry },
		HandleIsWorkflow:        func() bool { return true },
		HandleNow:               time.Now,
		HandleGetCurrentVersion: func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 {
			transitions++
			return transitions
		},
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: transitions}
		},
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey(tests.GlobalNamespaceEntry.ID().String(), callerWorkflowID, callerRunID)
		},
		HandleAddHistoryEvent: func(et enumspb.EventType, setAttrs func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
			e := rt.builder.AddHistoryEvent(et, setAttrs)
			rt.executed = append(rt.executed, e)
			return e
		},
		HandleHasAnyBufferedEvent: func(filter func(*historypb.HistoryEvent) bool) bool {
			return rt.builder.HasAnyBufferedEvent(filter)
		},
	}
	rt.root = chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := rt.mutableContext()
	require.NoError(t, rt.root.SetRootComponent(NewWorkflow(ctx, chasm.NewMSPointer(backend))))
	rt.commit()
	return rt
}

func (rt *resultTree) newBuilder(nextEventID int64, dbBuffered []*historypb.HistoryEvent) {
	rt.builder = historybuilder.New(clock.NewRealTimeSource(), func(n int) ([]int64, error) {
		return make([]int64, n), nil
	}, 1, nextEventID, dbBuffered, metrics.NoopMetricsHandler, dynamicconfig.GetIntPropertyFn(0))
}

func (rt *resultTree) mutableContext() chasm.MutableContext {
	goCtx := context.WithValue(context.Background(), nexusoperation.OperationContextKey, &nexusoperation.OperationContext{
		MetricTagConfig: dynamicconfig.GetTypedPropertyFn(nexusoperation.NexusMetricTagConfig{}),
	})
	return SetEventRegistryOnContext(chasm.NewMutableContext(goCtx, rt.root), rt.wfreg)
}

// transaction runs fn against the workflow and closes the CHASM transaction and history batch, like a mutable state
// transaction with no workflow task in flight.
func (rt *resultTree) transaction(fn func(ctx chasm.MutableContext, wf *Workflow) error) error {
	ctx := rt.mutableContext()
	component, err := rt.root.ComponentByPath(ctx, nil)
	require.NoError(rt.t, err)
	if err := fn(ctx, component.(*Workflow)); err != nil {
		return err
	}
	rt.commit()
	return nil
}

func (rt *resultTree) commit() {
	rt.t.Helper()
	_, err := rt.root.CloseTransaction(context.Background(), true)
	require.NoError(rt.t, err)
	mutation, err := rt.builder.Finish(true)
	require.NoError(rt.t, err)
	rt.batches = append(rt.batches, mutation.DBEventsBatches...)
	rt.newBuilder(rt.builder.NextEventID(), mutation.MemBufferBatch)
}

func (rt *resultTree) emitted() []enumspb.EventType {
	var types []enumspb.EventType
	for _, batch := range rt.batches {
		for _, e := range batch {
			types = append(types, e.EventType)
		}
	}
	return types
}

func (rt *resultTree) schedule(service, operation string) int64 {
	rt.t.Helper()
	handler, ok := rt.wfreg.CommandHandler(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION)
	require.True(rt.t, ok)
	payloads, err := sdkconverter.PreferProtoDataConverter.ToPayloads(resultRequest("target", ""))
	require.NoError(rt.t, err)
	if service != resultSvc.ServiceName {
		payloads, err = sdkconverter.PreferProtoDataConverter.ToPayloads(&emptypb.Empty{})
		require.NoError(rt.t, err)
	}
	var id int64
	require.NoError(rt.t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error {
		err := handler(ctx, wf, commandValidator{maxPayloadSize: 1000}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  commonnexus.SystemEndpoint,
					Service:   service,
					Operation: operation,
					Input:     payloads.Payloads[0],
				},
			},
		}, CommandHandlerOptions{WorkflowTaskCompletedEventID: 1})
		if err != nil {
			return err
		}
		id = rt.executed[len(rt.executed)-1].EventId
		return nil
	}))
	return id
}

func (rt *resultTree) scheduleResultWait() int64 {
	return rt.schedule(resultSvc.ServiceName, resultSvc.GetWorkflowExecutionResult.Name())
}

func (rt *resultTree) cancel(ctx chasm.MutableContext, wf *Workflow, id int64) error {
	handler, ok := rt.wfreg.CommandHandler(enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION)
	require.True(rt.t, ok)
	return handler(ctx, wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
		Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
			RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{ScheduledEventId: id},
		},
	}, CommandHandlerOptions{WorkflowTaskCompletedEventID: 1})
}

func (rt *resultTree) start(id int64, token string) {
	rt.t.Helper()
	require.NoError(rt.t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error {
		now := time.Now()
		return wf.OnNexusOperationStarted(ctx, wf.Operations[id].Get(ctx), token, &now, nil)
	}))
}

func (rt *resultTree) operation(id int64) (*nexusoperation.Operation, bool) {
	ctx := chasm.NewContext(context.Background(), rt.root)
	component, err := rt.root.ComponentByPath(ctx, nil)
	require.NoError(rt.t, err)
	field, ok := component.(*Workflow).Operations[id]
	if !ok {
		return nil, false
	}
	return field.Get(ctx), true
}

func TestGetWorkflowExecutionResultCancelRealTree(t *testing.T) {
	// The second cancel is not accepted by the CHASM handler; the dispatcher then hands it to the HSM handler, which
	// fails the workflow task because the operation exists in neither tree.
	t.Run("second cancel in the same workflow task is not accepted", func(t *testing.T) {
		rt := newResultTree(t)
		id := rt.scheduleResultWait()
		require.NoError(t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error {
			require.NoError(t, rt.cancel(ctx, wf, id))
			require.ErrorIs(t, rt.cancel(ctx, wf, id), ErrCommandTargetNotFound)
			return nil
		}))
		require.Equal(t, []enumspb.EventType{
			enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		}, rt.emitted())
		_, ok := rt.operation(id)
		require.False(t, ok)
	})

	t.Run("target ack records Canceled", func(t *testing.T) {
		rt := newResultTree(t)
		id := rt.scheduleResultWait()
		rt.start(id, "subscription-token")
		require.NoError(t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error { return rt.cancel(ctx, wf, id) }))
		op, ok := rt.operation(id)
		require.True(t, ok)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)

		require.NoError(t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error {
			return wf.OnNexusOperationCancellationCompleted(ctx, wf.Operations[id].Get(ctx))
		}))
		require.Equal(t, []enumspb.EventType{
			enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		}, rt.emitted())
		canceled := rt.executed[len(rt.executed)-1].GetNexusOperationCanceledEventAttributes()
		require.Equal(t, id, canceled.GetScheduledEventId())
		require.NotNil(t, canceled.GetFailure().GetCause().GetCanceledFailureInfo())
		_, ok = rt.operation(id)
		require.False(t, ok)
	})

	// DEC-5: the caller keeps waiting when the target rejects the cancel.
	t.Run("rejected cancel keeps waiting", func(t *testing.T) {
		rt := newResultTree(t)
		id := rt.scheduleResultWait()
		rt.start(id, "subscription-token")
		require.NoError(t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error { return rt.cancel(ctx, wf, id) }))
		require.NoError(t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error {
			return wf.OnNexusOperationCancellationFailed(ctx, wf.Operations[id].Get(ctx), &failurepb.Failure{Message: "rejected"})
		}))
		op, ok := rt.operation(id)
		require.True(t, ok)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
		require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED, rt.emitted()[len(rt.emitted())-1])
	})

	t.Run("other operations keep the deferred cancel", func(t *testing.T) {
		rt := newResultTree(t)
		id := rt.schedule("other-service", "op")
		require.NoError(t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error { return rt.cancel(ctx, wf, id) }))
		op, ok := rt.operation(id)
		require.True(t, ok)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, op.Status)

		rt.start(id, "token")
		op, _ = rt.operation(id)
		ctx := chasm.NewContext(context.Background(), rt.root)
		require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, op.Cancellation.Get(ctx).Status, "the cancel is sent once started")

		require.NoError(t, rt.transaction(func(ctx chasm.MutableContext, wf *Workflow) error {
			return wf.OnNexusOperationCancellationCompleted(ctx, wf.Operations[id].Get(ctx))
		}))
		op, ok = rt.operation(id)
		require.True(t, ok, "the handler reports the outcome")
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
	})
}
