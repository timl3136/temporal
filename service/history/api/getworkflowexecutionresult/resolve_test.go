package getworkflowexecutionresult

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type lockEvent struct {
	op    string
	runID string
	err   error
}

type fakeRun struct {
	mu           sync.Mutex
	key          definition.WorkflowKey
	loadFn       func(ctx context.Context) (historyi.MutableState, error)
	updateFn     func(ctx context.Context) error
	clearFn      func()
	updateCalls  int
	clearCalls   int
	acquireCalls int
}

type fakeWorkflowContext struct {
	historyi.WorkflowContext
	run *fakeRun
}

func (c *fakeWorkflowContext) GetWorkflowKey() definition.WorkflowKey {
	return c.run.key
}

func (c *fakeWorkflowContext) LoadMutableState(
	ctx context.Context,
	_ historyi.ShardContext,
) (historyi.MutableState, error) {
	return c.run.loadFn(ctx)
}

func (c *fakeWorkflowContext) Clear() {
	c.run.mu.Lock()
	c.run.clearCalls++
	clearFn := c.run.clearFn
	c.run.mu.Unlock()
	if clearFn != nil {
		clearFn()
	}
}

func (c *fakeWorkflowContext) UpdateWorkflowExecutionAsActive(
	ctx context.Context,
	_ historyi.ShardContext,
) error {
	c.run.mu.Lock()
	c.run.updateCalls++
	updateFn := c.run.updateFn
	c.run.mu.Unlock()
	if updateFn != nil {
		return updateFn(ctx)
	}
	return nil
}

type trackingWorkflowCache struct {
	mu               sync.Mutex
	runs             map[string]*fakeRun
	perRunLocks      map[string]*sync.Mutex
	heldRunLocks     int
	maxHeldRunLocks  int
	heldCurrentLocks int
	lockEvents       []lockEvent
}

func newTrackingWorkflowCache() *trackingWorkflowCache {
	return &trackingWorkflowCache{
		runs:        make(map[string]*fakeRun),
		perRunLocks: make(map[string]*sync.Mutex),
	}
}

func (c *trackingWorkflowCache) addRun(run *fakeRun) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runs[run.key.RunID] = run
	c.perRunLocks[run.key.RunID] = &sync.Mutex{}
}

func (c *trackingWorkflowCache) GetOrCreateCurrentExecution(
	_ context.Context,
	_ historyi.ShardContext,
	_ namespace.ID,
	_ string,
	_ chasm.ArchetypeID,
	_ locks.Priority,
) (historyi.ReleaseWorkflowContextFunc, error) {
	c.mu.Lock()
	c.heldCurrentLocks++
	c.mu.Unlock()

	var once sync.Once
	return func(error) {
		once.Do(func() {
			c.mu.Lock()
			c.heldCurrentLocks--
			c.mu.Unlock()
		})
	}, nil
}

func (c *trackingWorkflowCache) GetOrCreateWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	lockPriority locks.Priority,
) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {
	return c.GetOrCreateChasmExecution(ctx, shardContext, namespaceID, execution, chasm.WorkflowArchetypeID, lockPriority)
}

func (c *trackingWorkflowCache) GetOrCreateChasmExecution(
	_ context.Context,
	_ historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	_ chasm.ArchetypeID,
	_ locks.Priority,
) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {
	runID := execution.GetRunId()
	c.mu.Lock()
	run, ok := c.runs[runID]
	runMu, hasMu := c.perRunLocks[runID]
	if !hasMu {
		runMu = &sync.Mutex{}
		c.perRunLocks[runID] = runMu
	}
	c.mu.Unlock()

	runMu.Lock()

	c.mu.Lock()
	c.heldRunLocks++
	if c.heldRunLocks > c.maxHeldRunLocks {
		c.maxHeldRunLocks = c.heldRunLocks
	}
	c.lockEvents = append(c.lockEvents, lockEvent{op: "acquire", runID: runID})
	c.mu.Unlock()

	if !ok {
		run = &fakeRun{
			key: definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), runID),
			loadFn: func(context.Context) (historyi.MutableState, error) {
				return nil, serviceerror.NewNotFound("workflow execution not found")
			},
		}
	} else {
		run.mu.Lock()
		run.acquireCalls++
		run.mu.Unlock()
	}

	wfCtx := &fakeWorkflowContext{run: run}
	var once sync.Once
	releaseFn := func(err error) {
		once.Do(func() {
			if err != nil {
				wfCtx.Clear()
			}
			c.mu.Lock()
			c.heldRunLocks--
			c.lockEvents = append(c.lockEvents, lockEvent{op: "release", runID: runID, err: err})
			c.mu.Unlock()
			runMu.Unlock()
		})
	}
	return wfCtx, releaseFn, nil
}

func (c *trackingWorkflowCache) totalUpdateCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, run := range c.runs {
		run.mu.Lock()
		total += run.updateCalls
		run.mu.Unlock()
	}
	return total
}

func (c *trackingWorkflowCache) requireNoLeakedLocks(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Zero(t, c.heldRunLocks, "run lock leaked")
	require.Zero(t, c.heldCurrentLocks, "current execution lock leaked")
}

func (c *trackingWorkflowCache) requireSingleRunLockAtATime(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.LessOrEqual(t, c.maxHeldRunLocks, 1, "held more than one run lock concurrently")
}

func (c *trackingWorkflowCache) snapshotLockEvents() []lockEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]lockEvent, len(c.lockEvents))
	copy(out, c.lockEvents)
	return out
}

type currentExecutionResponse struct {
	runID string
	err   error
}

type resolveTestEnv struct {
	ctrl        *gomock.Controller
	shard       *historyi.MockShardContext
	cache       *trackingWorkflowCache
	checker     api.WorkflowConsistencyChecker
	namespaceID namespace.ID
	workflowID  string
	caller      *historyservice.NexusCallerInfo

	mu          sync.Mutex
	currentSeq  []currentExecutionResponse
	currentIdx  int
	defaultCurr currentExecutionResponse
}

func newResolveTestEnv(t *testing.T) *resolveTestEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockShard := historyi.NewMockShardContext(ctrl)
	mockShard.EXPECT().GetShardID().Return(int32(1)).AnyTimes()

	nsID := namespace.ID(uuid.NewString())
	wfID := "target-wf-" + uuid.NewString()
	cache := newTrackingWorkflowCache()
	env := &resolveTestEnv{
		ctrl:        ctrl,
		shard:       mockShard,
		cache:       cache,
		checker:     api.NewWorkflowConsistencyChecker(mockShard, cache),
		namespaceID: nsID,
		workflowID:  wfID,
		caller: &historyservice.NexusCallerInfo{
			NamespaceId: nsID.String(),
			WorkflowId:  "caller-wf-" + uuid.NewString(),
			RunId:       uuid.NewString(),
			RequestId:   uuid.NewString(),
		},
		defaultCurr: currentExecutionResponse{
			err: serviceerror.NewNotFound("current execution not found"),
		},
	}

	mockShard.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *persistence.GetCurrentExecutionRequest) (*persistence.GetCurrentExecutionResponse, error) {
			env.mu.Lock()
			defer env.mu.Unlock()
			var resp currentExecutionResponse
			if env.currentIdx < len(env.currentSeq) {
				resp = env.currentSeq[env.currentIdx]
				env.currentIdx++
			} else {
				resp = env.defaultCurr
			}
			if resp.err != nil {
				return nil, resp.err
			}
			return &persistence.GetCurrentExecutionResponse{RunID: resp.runID}, nil
		},
	).AnyTimes()

	return env
}

func (e *resolveTestEnv) setCurrentRun(runID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.defaultCurr = currentExecutionResponse{runID: runID}
}

func (e *resolveTestEnv) setCurrentSequence(seq ...currentExecutionResponse) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.currentSeq = seq
	e.currentIdx = 0
}

type mockRunSpec struct {
	runID             string
	firstRunID        string
	firstRunIDErr     error
	running           bool
	state             enumsspb.WorkflowExecutionState
	status            enumspb.WorkflowExecutionStatus
	newExecutionRunID string
	resetRunID        string
	workflowWasReset  bool
	successorRunID    string
	closeEvent        *historypb.HistoryEvent
	closeEventErr     error
}

func (e *resolveTestEnv) addMockRun(spec mockRunSpec) *fakeRun {
	ms := e.newMockMutableState(spec)
	run := &fakeRun{
		key: definition.NewWorkflowKey(e.namespaceID.String(), e.workflowID, spec.runID),
		loadFn: func(context.Context) (historyi.MutableState, error) {
			return ms, nil
		},
	}
	e.cache.addRun(run)
	return run
}

func (e *resolveTestEnv) newMockMutableState(spec mockRunSpec) *historyi.MockMutableState {
	ms := historyi.NewMockMutableState(e.ctrl)
	status := spec.status
	state := spec.state
	if state == enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED {
		if spec.running {
			state = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
		} else {
			state = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		}
	}
	if spec.running && status == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}
	execInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:         e.namespaceID.String(),
		WorkflowId:          e.workflowID,
		FirstExecutionRunId: spec.firstRunID,
		NewExecutionRunId:   spec.newExecutionRunID,
		ResetRunId:          spec.resetRunID,
		WorkflowWasReset:    spec.workflowWasReset,
		SuccessorRunId:      spec.successorRunID,
	}
	execState := &persistencespb.WorkflowExecutionState{
		RunId:               spec.runID,
		State:               state,
		Status:              status,
		FirstExecutionRunId: spec.firstRunID,
	}
	ms.EXPECT().IsWorkflowExecutionRunning().Return(spec.running).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(execState).AnyTimes()
	ms.EXPECT().GetFirstRunID(gomock.Any()).Return(spec.firstRunID, spec.firstRunIDErr).AnyTimes()
	if spec.closeEvent != nil || spec.closeEventErr != nil {
		ms.EXPECT().GetCompletionEvent(gomock.Any()).Return(spec.closeEvent, spec.closeEventErr).AnyTimes()
	}
	return ms
}

func makeCompletedEvent(data []byte) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   5,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				Result: &commonpb.Payloads{
					Payloads: []*commonpb.Payload{{
						Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
						Data:     data,
					}},
				},
			},
		},
	}
}

func makeTerminatedEvent(reason, identity string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   8,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason:   reason,
				Identity: identity,
			},
		},
	}
}

func TestResolveTargetValidation(t *testing.T) {
	t.Parallel()

	nsID := namespace.ID(uuid.NewString())
	callerWF := "wf-same"
	callerRun := uuid.NewString()
	validCaller := &historyservice.NexusCallerInfo{
		NamespaceId: nsID.String(),
		WorkflowId:  callerWF,
		RunId:       callerRun,
		RequestId:   uuid.NewString(),
	}

	testCases := []struct {
		name string
		req  resolveRequest
	}{
		{
			name: "empty namespace ID",
			req: resolveRequest{
				namespaceID: "",
				caller:      validCaller,
				target:      &commonpb.WorkflowExecution{WorkflowId: "target"},
			},
		},
		{
			name: "invalid namespace UUID",
			req: resolveRequest{
				namespaceID: "not-a-uuid",
				caller:      validCaller,
				target:      &commonpb.WorkflowExecution{WorkflowId: "target"},
			},
		},
		{
			name: "nil target",
			req: resolveRequest{
				namespaceID: nsID,
				caller:      validCaller,
				target:      nil,
			},
		},
		{
			name: "empty target workflow ID",
			req: resolveRequest{
				namespaceID: nsID,
				caller:      validCaller,
				target:      &commonpb.WorkflowExecution{},
			},
		},
		{
			name: "invalid target run ID UUID",
			req: resolveRequest{
				namespaceID: nsID,
				caller:      validCaller,
				target:      &commonpb.WorkflowExecution{WorkflowId: "target", RunId: "not-a-uuid"},
			},
		},
		{
			name: "nil caller",
			req: resolveRequest{
				namespaceID: nsID,
				caller:      nil,
				target:      &commonpb.WorkflowExecution{WorkflowId: "target"},
			},
		},
		{
			name: "missing caller workflow ID",
			req: resolveRequest{
				namespaceID: nsID,
				caller:      &historyservice.NexusCallerInfo{NamespaceId: nsID.String(), RunId: callerRun},
				target:      &commonpb.WorkflowExecution{WorkflowId: "target"},
			},
		},
		{
			name: "missing caller run ID",
			req: resolveRequest{
				namespaceID: nsID,
				caller:      &historyservice.NexusCallerInfo{NamespaceId: nsID.String(), WorkflowId: callerWF},
				target:      &commonpb.WorkflowExecution{WorkflowId: "target"},
			},
		},
		{
			name: "cross-namespace caller",
			req: resolveRequest{
				namespaceID: nsID,
				caller: &historyservice.NexusCallerInfo{
					NamespaceId: uuid.NewString(),
					WorkflowId:  callerWF,
					RunId:       callerRun,
				},
				target: &commonpb.WorkflowExecution{WorkflowId: "target"},
			},
		},
		{
			name: "direct self-wait with explicit run ID",
			req: resolveRequest{
				namespaceID: nsID,
				caller:      validCaller,
				target:      &commonpb.WorkflowExecution{WorkflowId: callerWF, RunId: callerRun},
			},
		},
		{
			name: "direct self-wait with empty run ID",
			req: resolveRequest{
				namespaceID: nsID,
				caller:      validCaller,
				target:      &commonpb.WorkflowExecution{WorkflowId: callerWF},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newResolveTestEnv(t)
			actionCalled := false
			_, err := resolveTarget(
				t.Context(),
				env.shard,
				env.checker,
				tc.req,
				func(api.WorkflowLease, resolvedTarget) (*api.UpdateWorkflowAction, error) {
					actionCalled = true
					return api.UpdateWorkflowWithoutWorkflowTask, nil
				},
			)
			var invalidArg *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArg)
			require.False(t, api.IsRetryableError(err))
			require.False(t, actionCalled)
			require.Empty(t, env.cache.snapshotLockEvents())
			require.Zero(t, env.cache.totalUpdateCalls())
			env.cache.requireNoLeakedLocks(t)
		})
	}
}

func TestResolveTargetMissing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		runID string
	}{
		{name: "missing workflow with empty run ID"},
		{name: "missing explicit run ID", runID: uuid.NewString()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newResolveTestEnv(t)
			actionCalled := false
			_, err := resolveTarget(
				t.Context(),
				env.shard,
				env.checker,
				resolveRequest{
					namespaceID: env.namespaceID,
					caller:      env.caller,
					target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: tc.runID},
				},
				func(api.WorkflowLease, resolvedTarget) (*api.UpdateWorkflowAction, error) {
					actionCalled = true
					return api.UpdateWorkflowWithoutWorkflowTask, nil
				},
			)
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound)
			require.False(t, api.IsRetryableError(err))
			require.False(t, actionCalled)
			require.Zero(t, env.cache.totalUpdateCalls())
			env.cache.requireNoLeakedLocks(t)
		})
	}
}

func TestResolveTargetDirectOpenAndClosed(t *testing.T) {
	t.Parallel()

	for _, explicit := range []bool{false, true} {
		name := "empty run ID open run invokes action under lease and persists"
		if explicit {
			name = "explicit open run invokes action under lease and persists"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			env := newResolveTestEnv(t)
			firstRunID := uuid.NewString()
			runID := uuid.NewString()
			run := env.addMockRun(mockRunSpec{
				runID:      runID,
				firstRunID: firstRunID,
				running:    true,
			})
			env.setCurrentRun(runID)

			targetRunID := ""
			if explicit {
				targetRunID = runID
			}
			var sawResolved resolvedTarget
			var sawHeldLock bool
			got, err := resolveTarget(
				t.Context(),
				env.shard,
				env.checker,
				resolveRequest{
					namespaceID: env.namespaceID,
					caller:      env.caller,
					target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: targetRunID},
				},
				func(_ api.WorkflowLease, target resolvedTarget) (*api.UpdateWorkflowAction, error) {
					env.cache.mu.Lock()
					sawHeldLock = env.cache.heldRunLocks == 1
					env.cache.mu.Unlock()
					sawResolved = target
					return api.UpdateWorkflowWithoutWorkflowTask, nil
				},
			)
			require.NoError(t, err)
			require.True(t, sawHeldLock)
			want := resolvedTarget{
				workflowKey: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, runID),
				firstRunID:  firstRunID,
			}
			require.Equal(t, want, got)
			require.Equal(t, want, sawResolved)
			require.Equal(t, 1, run.updateCalls)
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})
	}

	t.Run("closed terminal current run returns cloned close event and writes nothing", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		runID := uuid.NewString()
		origEvent := makeCompletedEvent([]byte("original-payload"))
		env.addMockRun(mockRunSpec{
			runID:      runID,
			firstRunID: runID,
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			closeEvent: origEvent,
		})
		env.setCurrentRun(runID)

		actionCalled := false
		got, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			},
			func(_ api.WorkflowLease, target resolvedTarget) (*api.UpdateWorkflowAction, error) {
				actionCalled = true
				require.NotNil(t, target.closeEvent)
				return &api.UpdateWorkflowAction{Noop: true}, nil
			},
		)
		require.NoError(t, err)
		require.True(t, actionCalled)
		require.Equal(t, definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, runID), got.workflowKey)
		require.Equal(t, runID, got.firstRunID)
		protorequire.ProtoEqual(t, origEvent, got.closeEvent)
		require.Zero(t, env.cache.totalUpdateCalls())

		// Mutating the returned close event must not mutate the original event in mutable state.
		got.closeEvent.GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0].Data[0] ^= 0xff
		require.Equal(t, []byte("original-payload"), origEvent.GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0].Data)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("closed read refuses mutating action without writing", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		runID := uuid.NewString()
		env.addMockRun(mockRunSpec{
			runID:      runID,
			firstRunID: runID,
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			closeEvent: makeCompletedEvent([]byte("done")),
		})
		env.setCurrentRun(runID)

		_, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			},
			func(api.WorkflowLease, resolvedTarget) (*api.UpdateWorkflowAction, error) {
				return api.UpdateWorkflowWithoutWorkflowTask, nil
			},
		)
		var internalErr *serviceerror.Internal
		require.ErrorAs(t, err, &internalErr)
		require.Zero(t, env.cache.totalUpdateCalls())
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("closed current run marked reset returns close event as final", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name        string
			explicitRun bool
			specFn      func(runID string) mockRunSpec
		}{
			{
				name: "empty run ID terminated with resetter identity",
				specFn: func(runID string) mockRunSpec {
					return mockRunSpec{
						runID:      runID,
						firstRunID: runID,
						status:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
						closeEvent: makeTerminatedEvent("user-terminated", consts.IdentityResetter),
					}
				},
			},
			{
				name: "empty run ID marked workflow_was_reset with reset run ID",
				specFn: func(runID string) mockRunSpec {
					return mockRunSpec{
						runID:            runID,
						firstRunID:       runID,
						status:           enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
						workflowWasReset: true,
						resetRunID:       uuid.NewString(),
						closeEvent:       makeTerminatedEvent("reset", consts.IdentityResetter),
					}
				},
			},
			{
				name:        "explicit run ID terminated with resetter identity while still current",
				explicitRun: true,
				specFn: func(runID string) mockRunSpec {
					return mockRunSpec{
						runID:      runID,
						firstRunID: runID,
						status:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
						closeEvent: makeTerminatedEvent("user-terminated", consts.IdentityResetter),
					}
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				env := newResolveTestEnv(t)
				runID := uuid.NewString()
				spec := tc.specFn(runID)
				env.addMockRun(spec)
				env.setCurrentRun(runID)

				targetRunID := ""
				if tc.explicitRun {
					targetRunID = runID
				}
				got, err := resolveTarget(
					t.Context(),
					env.shard,
					env.checker,
					resolveRequest{
						namespaceID: env.namespaceID,
						caller:      env.caller,
						target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: targetRunID},
					},
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, runID), got.workflowKey)
				require.Equal(t, runID, got.firstRunID)
				protorequire.ProtoEqual(t, spec.closeEvent, got.closeEvent)
				require.Zero(t, env.cache.totalUpdateCalls())
				env.cache.requireSingleRunLockAtATime(t)
				env.cache.requireNoLeakedLocks(t)
			})
		}
	})
}

func TestResolveTargetZombieRun(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		setup func(env *resolveTestEnv) string
	}{
		{
			name: "explicit zombie run returns FailedPrecondition without reading close event",
			setup: func(env *resolveTestEnv) string {
				r1 := uuid.NewString()
				// closeEvent is intentionally nil on strict mock so calling GetCompletionEvent fails the test.
				env.addMockRun(mockRunSpec{
					runID:      r1,
					firstRunID: r1,
					state:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
				})
				return r1
			},
		},
		{
			name: "zombie run reached during chain walk returns FailedPrecondition",
			setup: func(env *resolveTestEnv) string {
				c1 := uuid.NewString()
				c2 := uuid.NewString()
				r2 := uuid.NewString()
				env.addMockRun(mockRunSpec{
					runID:             c1,
					firstRunID:        c1,
					status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
					newExecutionRunID: c2,
				})
				env.addMockRun(mockRunSpec{
					runID:      c2,
					firstRunID: c1,
					state:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
				})
				env.addMockRun(mockRunSpec{
					runID:      r2,
					firstRunID: r2,
					running:    true,
				})
				env.setCurrentRun(r2)
				return c1
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newResolveTestEnv(t)
			targetRunID := tc.setup(env)
			actionCalled := false
			_, err := resolveTarget(
				t.Context(),
				env.shard,
				env.checker,
				resolveRequest{
					namespaceID: env.namespaceID,
					caller:      env.caller,
					target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: targetRunID},
				},
				func(api.WorkflowLease, resolvedTarget) (*api.UpdateWorkflowAction, error) {
					actionCalled = true
					return api.UpdateWorkflowWithoutWorkflowTask, nil
				},
			)
			var failedPrecond *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecond)
			require.False(t, api.IsRetryableError(err))
			require.False(t, actionCalled)
			require.Zero(t, env.cache.totalUpdateCalls())
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})
	}
}

func TestResolveTargetSuppliedTerminalRunWithNewerUnrelatedRun(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		status         enumspb.WorkflowExecutionStatus
		closeEvent     *historypb.HistoryEvent
		setSuccessorID bool // simulates TERMINATE_EXISTING setting SuccessorRunId to the unrelated run
		selfResetLoop  bool // simulates ResetRunId == runID self-pointer
	}{
		{
			name:       "completed",
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			closeEvent: makeCompletedEvent([]byte("v1")),
		},
		{
			name:   "failed without retry",
			status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			closeEvent: &historypb.HistoryEvent{
				EventId:   6,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						Failure: &failurepb.Failure{Message: "boom"},
					},
				},
			},
		},
		{
			name:   "canceled",
			status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
			closeEvent: &historypb.HistoryEvent{
				EventId:   7,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
					WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{},
				},
			},
		},
		{
			name:           "terminated by terminate-existing conflict policy",
			status:         enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			setSuccessorID: true,
			closeEvent:     makeTerminatedEvent("replaced", "client"),
		},
		{
			name:   "timed out without retry",
			status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
			closeEvent: &historypb.HistoryEvent{
				EventId:   9,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
					WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{},
				},
			},
		},
		{
			name:          "completed with self-pointing reset run ID",
			status:        enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			selfResetLoop: true,
			closeEvent:    makeCompletedEvent([]byte("self-reset-guard")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newResolveTestEnv(t)
			r1 := uuid.NewString()
			r2 := uuid.NewString()
			spec := mockRunSpec{
				runID:      r1,
				firstRunID: r1,
				status:     tc.status,
				closeEvent: tc.closeEvent,
			}
			if tc.setSuccessorID {
				spec.successorRunID = r2
			}
			if tc.selfResetLoop {
				spec.resetRunID = r1
			}
			env.addMockRun(spec)
			r2Run := env.addMockRun(mockRunSpec{
				runID:      r2,
				firstRunID: r2,
				running:    true,
			})
			env.setCurrentRun(r2)

			got, err := resolveTarget(
				t.Context(),
				env.shard,
				env.checker,
				resolveRequest{
					namespaceID: env.namespaceID,
					caller:      env.caller,
					target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: r1},
				},
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, r1), got.workflowKey)
			require.Equal(t, r1, got.firstRunID)
			protorequire.ProtoEqual(t, tc.closeEvent, got.closeEvent)
			require.Zero(t, r2Run.acquireCalls, "newer unrelated run must not be locked for a terminal target")
			require.Zero(t, env.cache.totalUpdateCalls())
			env.cache.requireNoLeakedLocks(t)
		})
	}

	t.Run("reused workflow ID caller awaiting prior terminal run is not a self-wait", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		r1 := uuid.NewString()
		r2 := uuid.NewString()
		origEvent := makeCompletedEvent([]byte("prior-result"))
		env.addMockRun(mockRunSpec{
			runID:      r1,
			firstRunID: r1,
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			closeEvent: origEvent,
		})
		env.addMockRun(mockRunSpec{
			runID:      r2,
			firstRunID: r2,
			running:    true,
		})
		env.setCurrentRun(r2)

		caller := &historyservice.NexusCallerInfo{
			NamespaceId: env.namespaceID.String(),
			WorkflowId:  env.workflowID,
			RunId:       r2,
			RequestId:   uuid.NewString(),
		}
		got, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: r1},
			},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, r1, got.workflowKey.RunID)
		protorequire.ProtoEqual(t, origEvent, got.closeEvent)
		require.Zero(t, env.cache.totalUpdateCalls())
		env.cache.requireNoLeakedLocks(t)
	})
}

func TestResolveTargetChainResolution(t *testing.T) {
	t.Parallel()

	t.Run("C1 to C2 to open C3 releases C1 before acquiring C3", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c3 := uuid.NewString()
		env.addMockRun(mockRunSpec{
			runID:             c1,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c2,
		})
		c2Run := env.addMockRun(mockRunSpec{
			runID:             c2,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c3,
		})
		c3Run := env.addMockRun(mockRunSpec{
			runID:      c3,
			firstRunID: c1,
			running:    true,
		})
		env.setCurrentRun(c3)

		got, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1},
			},
			func(api.WorkflowLease, resolvedTarget) (*api.UpdateWorkflowAction, error) {
				return api.UpdateWorkflowWithoutWorkflowTask, nil
			},
		)
		require.NoError(t, err)
		require.Equal(t, resolvedTarget{
			workflowKey: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c3),
			firstRunID:  c1,
		}, got)
		require.Zero(t, c2Run.acquireCalls, "intermediate run should not be locked when current is same chain")
		require.Equal(t, 1, c3Run.updateCalls)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
		require.Equal(t, []lockEvent{
			{op: "acquire", runID: c1},
			{op: "release", runID: c1, err: nil},
			{op: "acquire", runID: c3},
			{op: "release", runID: c3, err: nil},
		}, env.cache.snapshotLockEvents())
	})

	t.Run("C1 to C2 to terminal C3 returns cloned close event with no writes", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c3 := uuid.NewString()
		c3Close := makeCompletedEvent([]byte("c3-final"))
		env.addMockRun(mockRunSpec{
			runID:             c1,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c2,
		})
		env.addMockRun(mockRunSpec{
			runID:      c3,
			firstRunID: c1,
			status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			closeEvent: c3Close,
		})
		env.setCurrentRun(c3)

		got, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1},
			},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c3), got.workflowKey)
		require.Equal(t, c1, got.firstRunID)
		protorequire.ProtoEqual(t, c3Close, got.closeEvent)
		require.Zero(t, env.cache.totalUpdateCalls())
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("all superseded close variants resolve to current run of same chain", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name      string
			buildSpec func(c1, c2 string) mockRunSpec
		}{
			{
				name: "failed with retry successor",
				buildSpec: func(c1, c2 string) mockRunSpec {
					return mockRunSpec{
						runID:             c1,
						firstRunID:        c1,
						status:            enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
						newExecutionRunID: c2,
					}
				},
			},
			{
				name: "timed out with retry successor",
				buildSpec: func(c1, c2 string) mockRunSpec {
					return mockRunSpec{
						runID:             c1,
						firstRunID:        c1,
						status:            enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
						newExecutionRunID: c2,
					}
				},
			},
			{
				name: "completed with cron successor",
				buildSpec: func(c1, c2 string) mockRunSpec {
					return mockRunSpec{
						runID:             c1,
						firstRunID:        c1,
						status:            enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						newExecutionRunID: c2,
					}
				},
			},
			{
				name: "reset base run",
				buildSpec: func(c1, c2 string) mockRunSpec {
					return mockRunSpec{
						runID:            c1,
						firstRunID:       c1,
						status:           enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
						workflowWasReset: true,
						resetRunID:       c2,
					}
				},
			},
			{
				name: "reset terminated current run without reset run ID",
				buildSpec: func(c1, _ string) mockRunSpec {
					return mockRunSpec{
						runID:            c1,
						firstRunID:       c1,
						status:           enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
						workflowWasReset: true,
					}
				},
			},
			{
				name: "completed run reset after close",
				buildSpec: func(c1, c2 string) mockRunSpec {
					return mockRunSpec{
						runID:      c1,
						firstRunID: c1,
						status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						resetRunID: c2,
					}
				},
			},
			{
				name: "legacy close event with new execution run ID when execution info lacks it",
				buildSpec: func(c1, c2 string) mockRunSpec {
					return mockRunSpec{
						runID:      c1,
						firstRunID: c1,
						status:     enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
						closeEvent: &historypb.HistoryEvent{
							EventId:   4,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
							Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
								WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
									NewExecutionRunId: c2,
								},
							},
						},
					}
				},
			},
			{
				name: "legacy resetter termination event without workflow_was_reset flag",
				buildSpec: func(c1, _ string) mockRunSpec {
					return mockRunSpec{
						runID:      c1,
						firstRunID: c1,
						status:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
						closeEvent: makeTerminatedEvent("reset", consts.IdentityResetter),
					}
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				env := newResolveTestEnv(t)
				c1 := uuid.NewString()
				c2 := uuid.NewString()
				env.addMockRun(tc.buildSpec(c1, c2))
				env.addMockRun(mockRunSpec{
					runID:      c2,
					firstRunID: c1,
					running:    true,
				})
				env.setCurrentRun(c2)

				got, err := resolveTarget(
					t.Context(),
					env.shard,
					env.checker,
					resolveRequest{
						namespaceID: env.namespaceID,
						caller:      env.caller,
						target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1},
					},
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, resolvedTarget{
					workflowKey: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c2),
					firstRunID:  c1,
				}, got)
				env.cache.requireSingleRunLockAtATime(t)
				env.cache.requireNoLeakedLocks(t)
			})
		}
	})
}

func TestResolveTargetWalkAfterWorkflowIDReuse(t *testing.T) {
	t.Parallel()

	for _, callerIsR2 := range []bool{false, true} {
		name := "C1 pinned after R2 reuse walks C1 to C2 to C3 and returns C3 close event"
		if callerIsR2 {
			name = "caller R2 targeting old chain C1 is not a self-wait and returns C3 close event"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			env := newResolveTestEnv(t)
			c1 := uuid.NewString()
			c2 := uuid.NewString()
			c3 := uuid.NewString()
			r2 := uuid.NewString()
			c3Close := makeCompletedEvent([]byte("chain-c3-result"))

			env.addMockRun(mockRunSpec{
				runID:             c1,
				firstRunID:        c1,
				status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
				newExecutionRunID: c2,
			})
			env.addMockRun(mockRunSpec{
				runID:             c2,
				firstRunID:        c1,
				status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
				newExecutionRunID: c3,
			})
			env.addMockRun(mockRunSpec{
				runID:      c3,
				firstRunID: c1,
				status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				closeEvent: c3Close,
			})
			env.addMockRun(mockRunSpec{
				runID:      r2,
				firstRunID: r2,
				running:    true,
			})
			env.setCurrentRun(r2)

			caller := env.caller
			if callerIsR2 {
				caller = &historyservice.NexusCallerInfo{
					NamespaceId: env.namespaceID.String(),
					WorkflowId:  env.workflowID,
					RunId:       r2,
					RequestId:   uuid.NewString(),
				}
			}

			got, err := resolveTarget(
				t.Context(),
				env.shard,
				env.checker,
				resolveRequest{
					namespaceID: env.namespaceID,
					caller:      caller,
					target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1},
				},
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c3), got.workflowKey)
			require.Equal(t, c1, got.firstRunID)
			protorequire.ProtoEqual(t, c3Close, got.closeEvent)
			require.Zero(t, env.cache.totalUpdateCalls())
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
			require.Equal(t, []lockEvent{
				{op: "acquire", runID: c1},
				{op: "release", runID: c1, err: nil},
				{op: "acquire", runID: r2},
				{op: "release", runID: r2, err: nil},
				{op: "acquire", runID: c2},
				{op: "release", runID: c2, err: nil},
				{op: "acquire", runID: c3},
				{op: "release", runID: c3, err: nil},
			}, env.cache.snapshotLockEvents())
		})
	}

	t.Run("walk prefers ResetRunId over NewExecutionRunId and supports open tail run", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c3 := uuid.NewString()
		r2 := uuid.NewString()

		env.addMockRun(mockRunSpec{
			runID:             c1,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c2,
			resetRunID:        c3,
		})
		c2Run := env.addMockRun(mockRunSpec{
			runID:            c2,
			firstRunID:       c1,
			status:           enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			workflowWasReset: true,
		})
		c3Run := env.addMockRun(mockRunSpec{
			runID:      c3,
			firstRunID: c1,
			running:    true,
		})
		env.addMockRun(mockRunSpec{
			runID:      r2,
			firstRunID: r2,
			running:    true,
		})
		env.setCurrentRun(r2)

		got, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1},
			},
			func(api.WorkflowLease, resolvedTarget) (*api.UpdateWorkflowAction, error) {
				return api.UpdateWorkflowWithoutWorkflowTask, nil
			},
		)
		require.NoError(t, err)
		require.Equal(t, resolvedTarget{
			workflowKey: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c3),
			firstRunID:  c1,
		}, got)
		require.Zero(t, c2Run.acquireCalls, "reset branch should bypass abandoned continuation C2")
		require.Equal(t, 1, c3Run.updateCalls)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("broken chain walk returns non-retryable FailedPrecondition", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name  string
			setup func(env *resolveTestEnv) string
		}{
			{
				name: "deleted middle run C2",
				setup: func(env *resolveTestEnv) string {
					c1 := uuid.NewString()
					c2 := uuid.NewString()
					c3 := uuid.NewString()
					r2 := uuid.NewString()
					env.addMockRun(mockRunSpec{
						runID:             c1,
						firstRunID:        c1,
						status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
						newExecutionRunID: c2,
					})
					// c2 intentionally omitted to simulate retention deletion.
					env.addMockRun(mockRunSpec{
						runID:      c3,
						firstRunID: c1,
						status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						closeEvent: makeCompletedEvent([]byte("unreachable")),
					})
					env.addMockRun(mockRunSpec{
						runID:      r2,
						firstRunID: r2,
						running:    true,
					})
					env.setCurrentRun(r2)
					return c1
				},
			},
			{
				name: "superseded run with missing current execution and deleted successor",
				setup: func(env *resolveTestEnv) string {
					c1 := uuid.NewString()
					c2 := uuid.NewString()
					env.addMockRun(mockRunSpec{
						runID:             c1,
						firstRunID:        c1,
						status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
						newExecutionRunID: c2,
					})
					// Default current execution is NotFound and c2 is absent.
					return c1
				},
			},
			{
				name: "superseded run with no forward successor pointer after reuse",
				setup: func(env *resolveTestEnv) string {
					c1 := uuid.NewString()
					r2 := uuid.NewString()
					env.addMockRun(mockRunSpec{
						runID:            c1,
						firstRunID:       c1,
						status:           enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
						workflowWasReset: true,
					})
					env.addMockRun(mockRunSpec{
						runID:      r2,
						firstRunID: r2,
						running:    true,
					})
					env.setCurrentRun(r2)
					return c1
				},
			},
			{
				name: "cycle in successor pointers",
				setup: func(env *resolveTestEnv) string {
					c1 := uuid.NewString()
					c2 := uuid.NewString()
					r2 := uuid.NewString()
					env.addMockRun(mockRunSpec{
						runID:             c1,
						firstRunID:        c1,
						status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
						newExecutionRunID: c2,
					})
					env.addMockRun(mockRunSpec{
						runID:             c2,
						firstRunID:        c1,
						status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
						newExecutionRunID: c1,
					})
					env.addMockRun(mockRunSpec{
						runID:      r2,
						firstRunID: r2,
						running:    true,
					})
					env.setCurrentRun(r2)
					return c1
				},
			},
			{
				name: "hop limit exceeded",
				setup: func(env *resolveTestEnv) string {
					runIDs := make([]string, maxChainWalkHops+2)
					for i := range runIDs {
						runIDs[i] = uuid.NewString()
					}
					for i := range maxChainWalkHops + 1 {
						env.addMockRun(mockRunSpec{
							runID:             runIDs[i],
							firstRunID:        runIDs[0],
							status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
							newExecutionRunID: runIDs[i+1],
						})
					}
					env.addMockRun(mockRunSpec{
						runID:      runIDs[maxChainWalkHops+1],
						firstRunID: runIDs[0],
						status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						closeEvent: makeCompletedEvent([]byte("too-far")),
					})
					r2 := uuid.NewString()
					env.addMockRun(mockRunSpec{
						runID:      r2,
						firstRunID: r2,
						running:    true,
					})
					env.setCurrentRun(r2)
					return runIDs[0]
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				env := newResolveTestEnv(t)
				startRunID := tc.setup(env)
				actionCalled := false
				_, err := resolveTarget(
					t.Context(),
					env.shard,
					env.checker,
					resolveRequest{
						namespaceID: env.namespaceID,
						caller:      env.caller,
						target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: startRunID},
					},
					func(api.WorkflowLease, resolvedTarget) (*api.UpdateWorkflowAction, error) {
						actionCalled = true
						return api.UpdateWorkflowWithoutWorkflowTask, nil
					},
				)
				var failedPrecond *serviceerror.FailedPrecondition
				require.ErrorAs(t, err, &failedPrecond)
				require.False(t, api.IsRetryableError(err))
				require.False(t, actionCalled)
				require.Zero(t, env.cache.totalUpdateCalls())
				env.cache.requireSingleRunLockAtATime(t)
				env.cache.requireNoLeakedLocks(t)
			})
		}
	})
}

func TestResolveTargetSelfWaitAliases(t *testing.T) {
	t.Parallel()

	env := newResolveTestEnv(t)
	c1 := uuid.NewString()
	c2 := uuid.NewString()
	c3 := uuid.NewString()
	env.addMockRun(mockRunSpec{
		runID:             c1,
		firstRunID:        c1,
		status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		newExecutionRunID: c2,
	})
	env.addMockRun(mockRunSpec{
		runID:      c3,
		firstRunID: c1,
		running:    true,
	})
	env.setCurrentRun(c3)

	caller := &historyservice.NexusCallerInfo{
		NamespaceId: env.namespaceID.String(),
		WorkflowId:  env.workflowID,
		RunId:       c3,
		RequestId:   uuid.NewString(),
	}
	actionCalled := false
	_, err := resolveTarget(
		t.Context(),
		env.shard,
		env.checker,
		resolveRequest{
			namespaceID: env.namespaceID,
			caller:      caller,
			target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1},
		},
		func(api.WorkflowLease, resolvedTarget) (*api.UpdateWorkflowAction, error) {
			actionCalled = true
			return api.UpdateWorkflowWithoutWorkflowTask, nil
		},
	)
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
	require.False(t, api.IsRetryableError(err))
	require.False(t, actionCalled)
	require.Zero(t, env.cache.totalUpdateCalls())
	env.cache.requireSingleRunLockAtATime(t)
	env.cache.requireNoLeakedLocks(t)
	require.Equal(t, []lockEvent{
		{op: "acquire", runID: c1},
		{op: "release", runID: c1, err: nil},
		{op: "acquire", runID: c3},
		{op: "release", runID: c3, err: nil},
	}, env.cache.snapshotLockEvents())
}

func TestResolveTargetStaleCurrentRunRetry(t *testing.T) {
	t.Parallel()

	t.Run("empty run ID retries after checker releases stale current lease", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c3 := uuid.NewString()
		env.addMockRun(mockRunSpec{
			runID:             c2,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c3,
		})
		env.addMockRun(mockRunSpec{
			runID:      c3,
			firstRunID: c1,
			running:    true,
		})
		env.setCurrentSequence(
			currentExecutionResponse{runID: c2},
			currentExecutionResponse{runID: c3},
		)
		env.setCurrentRun(c3)

		got, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, resolvedTarget{
			workflowKey: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c3),
			firstRunID:  c1,
		}, got)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
		require.Equal(t, []lockEvent{
			{op: "acquire", runID: c2},
			{op: "release", runID: c2, err: nil},
			{op: "acquire", runID: c3},
			{op: "release", runID: c3, err: nil},
		}, env.cache.snapshotLockEvents())
	})

	t.Run("superseded run retries stale current run without relocking requested run", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c3 := uuid.NewString()
		c1Run := env.addMockRun(mockRunSpec{
			runID:             c1,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c2,
		})
		env.addMockRun(mockRunSpec{
			runID:             c2,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c3,
		})
		env.addMockRun(mockRunSpec{
			runID:      c3,
			firstRunID: c1,
			running:    true,
		})
		env.setCurrentSequence(
			currentExecutionResponse{runID: c2},
			currentExecutionResponse{runID: c2},
		)
		env.setCurrentRun(c3)

		got, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1},
			},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, resolvedTarget{
			workflowKey: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c3),
			firstRunID:  c1,
		}, got)
		require.Equal(t, 1, c1Run.acquireCalls, "requested run should only be locked once")
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
		require.Equal(t, []lockEvent{
			{op: "acquire", runID: c1},
			{op: "release", runID: c1, err: nil},
			{op: "acquire", runID: c2},
			{op: "release", runID: c2, err: nil},
			{op: "acquire", runID: c3},
			{op: "release", runID: c3, err: nil},
		}, env.cache.snapshotLockEvents())
	})

	t.Run("mutation conflict on stale explicit open run reloads and follows chain to C3", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c3 := uuid.NewString()

		staleOpenC2 := env.newMockMutableState(mockRunSpec{
			runID:      c2,
			firstRunID: c1,
			running:    true,
		})
		freshClosedC2 := env.newMockMutableState(mockRunSpec{
			runID:             c2,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c3,
		})
		conflictErr := &persistence.CurrentWorkflowConditionFailedError{
			Msg:   "current run moved to c3",
			RunID: c3,
		}

		c2Cleared := false
		c2Run := &fakeRun{
			key: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c2),
			loadFn: func(context.Context) (historyi.MutableState, error) {
				if c2Cleared {
					return freshClosedC2, nil
				}
				return staleOpenC2, nil
			},
			clearFn: func() {
				c2Cleared = true
			},
			updateFn: func(context.Context) error {
				return conflictErr
			},
		}
		env.cache.addRun(c2Run)
		c3Run := env.addMockRun(mockRunSpec{
			runID:      c3,
			firstRunID: c1,
			running:    true,
		})
		env.setCurrentRun(c3)

		var mutatedRuns []string
		got, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c2},
			},
			func(_ api.WorkflowLease, target resolvedTarget) (*api.UpdateWorkflowAction, error) {
				mutatedRuns = append(mutatedRuns, target.workflowKey.RunID)
				return api.UpdateWorkflowWithoutWorkflowTask, nil
			},
		)
		require.NoError(t, err)
		require.True(t, c2Cleared)
		require.Equal(t, []string{c2, c3}, mutatedRuns)
		require.Equal(t, resolvedTarget{
			workflowKey: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, c3),
			firstRunID:  c1,
		}, got)
		require.Equal(t, 1, c3Run.updateCalls)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
		require.Equal(t, []lockEvent{
			{op: "acquire", runID: c2},
			{op: "release", runID: c2, err: conflictErr},
			{op: "acquire", runID: c2},
			{op: "release", runID: c2, err: nil},
			{op: "acquire", runID: c3},
			{op: "release", runID: c3, err: nil},
		}, env.cache.snapshotLockEvents())
	})

	t.Run("bounded retries return ErrLocateCurrentWorkflowExecution when current stays stale", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c1Run := env.addMockRun(mockRunSpec{
			runID:             c1,
			firstRunID:        c1,
			status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			newExecutionRunID: c2,
		})
		env.setCurrentRun(c1)

		_, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			},
			nil,
		)
		require.ErrorIs(t, err, consts.ErrLocateCurrentWorkflowExecution)
		require.Equal(t, maxStaleCurrentRunAttempts, c1Run.acquireCalls)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("canceled context stops retries without acquiring a lease", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := resolveTarget(
			ctx,
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			},
			nil,
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, env.cache.snapshotLockEvents())
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("missing completion event propagates error without writing", func(t *testing.T) {
		t.Parallel()

		env := newResolveTestEnv(t)
		r1 := uuid.NewString()
		env.addMockRun(mockRunSpec{
			runID:         r1,
			firstRunID:    r1,
			status:        enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			closeEventErr: workflow.ErrMissingWorkflowCompletionEvent,
		})

		_, err := resolveTarget(
			t.Context(),
			env.shard,
			env.checker,
			resolveRequest{
				namespaceID: env.namespaceID,
				caller:      env.caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: r1},
			},
			nil,
		)
		require.ErrorIs(t, err, workflow.ErrMissingWorkflowCompletionEvent)
		require.Zero(t, env.cache.totalUpdateCalls())
		env.cache.requireNoLeakedLocks(t)
	})
}

// buildLegacyMutableState constructs a real MutableStateImpl with empty FirstExecutionRunId on both
// ExecutionState and ExecutionInfo so GetFirstRunID must load the WorkflowExecutionStarted event.
func buildLegacyMutableState(
	t *testing.T,
	testShard *shard.ContextTest,
	workflowID string,
	runID string,
	startEventFirstRunID string,
	running bool,
	newExecutionRunID string,
	closeEvent *historypb.HistoryEvent,
) historyi.MutableState {
	t.Helper()

	branchToken := []byte("branch-" + runID)
	state := enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	status := enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	if running {
		state = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
		status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	} else if newExecutionRunID == "" {
		status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}

	nextEventID := int64(3)
	lastEventID := int64(2)
	completionBatchID := int64(0)
	if closeEvent != nil {
		lastEventID = closeEvent.GetEventId()
		nextEventID = lastEventID + 1
		completionBatchID = lastEventID
	}

	dbState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:            tests.NamespaceID.String(),
			WorkflowId:             workflowID,
			NewExecutionRunId:      newExecutionRunID,
			CompletionEventBatchId: completionBatchID,
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: lastEventID, Version: tests.Version},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  runID,
			State:  state,
			Status: status,
		},
		NextEventId: nextEventID,
	}

	ms, err := workflow.NewMutableStateFromDB(
		testShard,
		testShard.MockEventsCache,
		testShard.GetLogger(),
		tests.LocalNamespaceEntry,
		dbState,
		1,
	)
	require.NoError(t, err)
	require.Empty(t, ms.GetExecutionInfo().GetFirstExecutionRunId())
	require.Empty(t, ms.GetExecutionState().GetFirstExecutionRunId())

	startEventKey := events.EventKey{
		NamespaceID: tests.NamespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		EventID:     common.FirstEventID,
		Version:     tests.Version,
		ShardUUID:   testShard.GetOwner(),
	}
	startEvent := &historypb.HistoryEvent{
		EventId:   common.FirstEventID,
		Version:   tests.Version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				FirstExecutionRunId: startEventFirstRunID,
			},
		},
	}
	testShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		testShard.GetShardID(),
		startEventKey,
		common.FirstEventID,
		branchToken,
	).Return(startEvent, nil)

	if closeEvent != nil {
		closeEventKey := events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			EventID:     closeEvent.GetEventId(),
			Version:     tests.Version,
			ShardUUID:   testShard.GetOwner(),
		}
		testShard.MockEventsCache.EXPECT().GetEvent(
			gomock.Any(),
			testShard.GetShardID(),
			closeEventKey,
			completionBatchID,
			branchToken,
		).Return(closeEvent, nil)
	}

	return ms
}

func newLegacyTestShard(t *testing.T) *shard.ContextTest {
	t.Helper()
	ctrl := gomock.NewController(t)
	cfg := tests.NewDynamicConfig()
	testShard := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
		cfg,
	)
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	testShard.SetStateMachineRegistry(reg)

	testShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	return testShard
}

func TestResolveTargetLegacyStartEventFallback(t *testing.T) {
	t.Parallel()

	t.Run("same chain resolved through start-event fallback on both runs", func(t *testing.T) {
		t.Parallel()

		testShard := newLegacyTestShard(t)
		wfID := "legacy-wf-" + uuid.NewString()
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c3 := uuid.NewString()

		c1MS := buildLegacyMutableState(t, testShard, wfID, c1, c1, false, c2, nil)
		c3MS := buildLegacyMutableState(t, testShard, wfID, c3, c1, true, "", nil)

		cache := newTrackingWorkflowCache()
		cache.addRun(&fakeRun{
			key:    definition.NewWorkflowKey(tests.NamespaceID.String(), wfID, c1),
			loadFn: func(context.Context) (historyi.MutableState, error) { return c1MS, nil },
		})
		cache.addRun(&fakeRun{
			key:    definition.NewWorkflowKey(tests.NamespaceID.String(), wfID, c3),
			loadFn: func(context.Context) (historyi.MutableState, error) { return c3MS, nil },
		})
		testShard.Resource.ExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(
			&persistence.GetCurrentExecutionResponse{RunID: c3},
			nil,
		)

		checker := api.NewWorkflowConsistencyChecker(testShard, cache)
		caller := &historyservice.NexusCallerInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  "caller-" + uuid.NewString(),
			RunId:       uuid.NewString(),
			RequestId:   uuid.NewString(),
		}

		got, err := resolveTarget(
			t.Context(),
			testShard,
			checker,
			resolveRequest{
				namespaceID: tests.NamespaceID,
				caller:      caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: c1},
			},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, resolvedTarget{
			workflowKey: definition.NewWorkflowKey(tests.NamespaceID.String(), wfID, c3),
			firstRunID:  c1,
		}, got)
		cache.requireSingleRunLockAtATime(t)
		cache.requireNoLeakedLocks(t)
	})

	t.Run("different chain detected via start-event fallback triggers walk to C3", func(t *testing.T) {
		t.Parallel()

		testShard := newLegacyTestShard(t)
		wfID := "legacy-reused-wf-" + uuid.NewString()
		c1 := uuid.NewString()
		c2 := uuid.NewString()
		c3 := uuid.NewString()
		r2 := uuid.NewString()
		c3Close := makeCompletedEvent([]byte("legacy-c3-close"))

		c1MS := buildLegacyMutableState(t, testShard, wfID, c1, c1, false, c2, nil)
		c2MS := buildLegacyMutableState(t, testShard, wfID, c2, c1, false, c3, nil)
		c3MS := buildLegacyMutableState(t, testShard, wfID, c3, c1, false, "", c3Close)
		r2MS := buildLegacyMutableState(t, testShard, wfID, r2, r2, true, "", nil)

		cache := newTrackingWorkflowCache()
		cache.addRun(&fakeRun{
			key:    definition.NewWorkflowKey(tests.NamespaceID.String(), wfID, c1),
			loadFn: func(context.Context) (historyi.MutableState, error) { return c1MS, nil },
		})
		cache.addRun(&fakeRun{
			key:    definition.NewWorkflowKey(tests.NamespaceID.String(), wfID, c2),
			loadFn: func(context.Context) (historyi.MutableState, error) { return c2MS, nil },
		})
		cache.addRun(&fakeRun{
			key:    definition.NewWorkflowKey(tests.NamespaceID.String(), wfID, c3),
			loadFn: func(context.Context) (historyi.MutableState, error) { return c3MS, nil },
		})
		cache.addRun(&fakeRun{
			key:    definition.NewWorkflowKey(tests.NamespaceID.String(), wfID, r2),
			loadFn: func(context.Context) (historyi.MutableState, error) { return r2MS, nil },
		})
		testShard.Resource.ExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(
			&persistence.GetCurrentExecutionResponse{RunID: r2},
			nil,
		)

		checker := api.NewWorkflowConsistencyChecker(testShard, cache)
		caller := &historyservice.NexusCallerInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  "caller-" + uuid.NewString(),
			RunId:       uuid.NewString(),
			RequestId:   uuid.NewString(),
		}

		got, err := resolveTarget(
			t.Context(),
			testShard,
			checker,
			resolveRequest{
				namespaceID: tests.NamespaceID,
				caller:      caller,
				target:      &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: c1},
			},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, definition.NewWorkflowKey(tests.NamespaceID.String(), wfID, c3), got.workflowKey)
		require.Equal(t, c1, got.firstRunID)
		protorequire.ProtoEqual(t, c3Close, got.closeEvent)
		cache.requireSingleRunLockAtATime(t)
		cache.requireNoLeakedLocks(t)
	})
}

func TestResolveTargetConcurrent(t *testing.T) {
	t.Parallel()

	env := newResolveTestEnv(t)
	c1 := uuid.NewString()
	c2 := uuid.NewString()
	c3 := uuid.NewString()
	closeEvent := makeCompletedEvent([]byte("concurrent-result"))
	env.addMockRun(mockRunSpec{
		runID:             c1,
		firstRunID:        c1,
		status:            enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		newExecutionRunID: c2,
	})
	env.addMockRun(mockRunSpec{
		runID:      c3,
		firstRunID: c1,
		status:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		closeEvent: closeEvent,
	})
	env.setCurrentRun(c3)

	const workers = 16
	type outcome struct {
		resolved resolvedTarget
		err      error
	}
	results := make(chan outcome, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		targetRunID := c1
		if i%2 == 1 {
			targetRunID = ""
		}
		go func() {
			defer wg.Done()
			got, err := resolveTarget(
				t.Context(),
				env.shard,
				env.checker,
				resolveRequest{
					namespaceID: env.namespaceID,
					caller:      env.caller,
					target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: targetRunID},
				},
				nil,
			)
			results <- outcome{resolved: got, err: err}
		}()
	}
	wg.Wait()
	close(results)

	for out := range results {
		require.NoError(t, out.err)
		require.Equal(t, c3, out.resolved.workflowKey.RunID)
		require.Equal(t, c1, out.resolved.firstRunID)
		protorequire.ProtoEqual(t, closeEvent, out.resolved.closeEvent)
		// Mutate each returned clone concurrently to confirm no aliasing with cached state.
		out.resolved.closeEvent.GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0].Data[0] ^= 0xaa
	}
	require.Equal(t, []byte("concurrent-result"), closeEvent.GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0].Data)
	require.Zero(t, env.cache.totalUpdateCalls())
	env.cache.requireNoLeakedLocks(t)
}
