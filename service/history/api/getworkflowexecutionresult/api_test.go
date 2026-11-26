package getworkflowexecutionresult

import (
	"context"
	"encoding/base64"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/persistence"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	hsmcallbacks "go.temporal.io/server/service/history/hsm/callbacks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type stubUpdateRegistry struct {
	update.Registry
	mu       sync.Mutex
	inFlight []chasmworkflow.CallbackAddition
}

func (r *stubUpdateRegistry) InFlightCallbacks() ([]chasmworkflow.CallbackAddition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]chasmworkflow.CallbackAddition, len(r.inFlight))
	copy(out, r.inFlight)
	return out, nil
}

func (r *stubUpdateRegistry) setInFlight(additions ...chasmworkflow.CallbackAddition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight = additions
}

type apiRun struct {
	mu           sync.Mutex
	key          definition.WorkflowKey
	ms           historyi.MutableState
	loadFn       func(ctx context.Context) (historyi.MutableState, error)
	updateFn     func(ctx context.Context, ms historyi.MutableState) error
	clearFn      func()
	updateReg    *stubUpdateRegistry
	updateCalls  int
	clearCalls   int
	acquireCalls int
	// initEvents tracks HistoryEvents committed during initial run creation.
	initEvents []*historypb.HistoryEvent
	// committedEvents tracks HistoryEvents produced on UpdateWorkflowExecutionAsActive.
	committedEvents []*historypb.HistoryEvent
}

type apiWorkflowContext struct {
	historyi.WorkflowContext
	run *apiRun
}

func (c *apiWorkflowContext) GetWorkflowKey() definition.WorkflowKey {
	return c.run.key
}

func (c *apiWorkflowContext) LoadMutableState(ctx context.Context, _ historyi.ShardContext) (historyi.MutableState, error) {
	if c.run.loadFn != nil {
		return c.run.loadFn(ctx)
	}
	return c.run.ms, nil
}

func (c *apiWorkflowContext) Clear() {
	c.run.mu.Lock()
	c.run.clearCalls++
	clearFn := c.run.clearFn
	c.run.mu.Unlock()
	if clearFn != nil {
		clearFn()
	}
}

func (c *apiWorkflowContext) UpdateRegistry(_ context.Context) update.Registry {
	return c.run.updateReg
}

func (c *apiWorkflowContext) UpdateWorkflowExecutionAsActive(
	ctx context.Context,
	_ historyi.ShardContext,
) error {
	c.run.mu.Lock()
	c.run.updateCalls++
	updateFn := c.run.updateFn
	ms := c.run.ms
	c.run.mu.Unlock()

	if updateFn != nil {
		return updateFn(ctx, ms)
	}
	if realMS, ok := ms.(*workflow.MutableStateImpl); ok {
		_, batches, err := realMS.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
		if err != nil {
			return err
		}
		c.run.mu.Lock()
		for _, batch := range batches {
			c.run.committedEvents = append(c.run.committedEvents, batch.Events...)
		}
		c.run.mu.Unlock()
	}
	return nil
}

type apiWorkflowCache struct {
	mu               sync.Mutex
	runs             map[string]*apiRun
	perRunLocks      map[string]*sync.Mutex
	heldRunLocks     int
	maxHeldRunLocks  int
	heldCurrentLocks int
	lockEvents       []lockEvent
}

func newAPIWorkflowCache() *apiWorkflowCache {
	return &apiWorkflowCache{
		runs:        make(map[string]*apiRun),
		perRunLocks: make(map[string]*sync.Mutex),
	}
}

func (c *apiWorkflowCache) addRun(run *apiRun) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if run.updateReg == nil {
		run.updateReg = &stubUpdateRegistry{}
	}
	c.runs[run.key.RunID] = run
	if _, ok := c.perRunLocks[run.key.RunID]; !ok {
		c.perRunLocks[run.key.RunID] = &sync.Mutex{}
	}
}

func (c *apiWorkflowCache) GetOrCreateCurrentExecution(
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

func (c *apiWorkflowCache) GetOrCreateWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	lockPriority locks.Priority,
) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {
	return c.GetOrCreateChasmExecution(ctx, shardContext, namespaceID, execution, chasm.WorkflowArchetypeID, lockPriority)
}

func (c *apiWorkflowCache) GetOrCreateChasmExecution(
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
		run = &apiRun{
			key:       definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), runID),
			updateReg: &stubUpdateRegistry{},
			loadFn: func(context.Context) (historyi.MutableState, error) {
				return nil, serviceerror.NewNotFound("workflow execution not found")
			},
		}
	} else {
		run.mu.Lock()
		run.acquireCalls++
		run.mu.Unlock()
	}

	wfCtx := &apiWorkflowContext{run: run}
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

func (c *apiWorkflowCache) totalUpdateCalls() int {
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

func (c *apiWorkflowCache) requireNoLeakedLocks(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Zero(t, c.heldRunLocks, "run lock leaked")
	require.Zero(t, c.heldCurrentLocks, "current execution lock leaked")
}

func (c *apiWorkflowCache) requireSingleRunLockAtATime(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.LessOrEqual(t, c.maxHeldRunLocks, 1, "held more than one run lock concurrently")
}

type apiTestEnv struct {
	t           *testing.T
	ctrl        *gomock.Controller
	testShard   *shard.ContextTest
	cfg         *configs.Config
	cache       *apiWorkflowCache
	checker     api.WorkflowConsistencyChecker
	namespaceID namespace.ID
	nsEntry     *namespace.Namespace
	workflowID  string
	caller      *historyservice.NexusCallerInfo

	mu          sync.Mutex
	currentSeq  []currentExecutionResponse
	currentIdx  int
	defaultCurr currentExecutionResponse
	events      map[string]map[int64]*historypb.HistoryEvent
}

func newAPITestEnv(t *testing.T, chasmCallbacks bool) *apiTestEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	cfg := tests.NewDynamicConfig()
	cfg.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	cfg.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(chasmCallbacks)
	cfg.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	cfg.EnableGetWorkflowExecutionResultFromWorkflow = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	testShard := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
		cfg,
	)

	hsmReg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(hsmReg))
	require.NoError(t, hsmcallbacks.RegisterStateMachine(hsmReg))
	require.NoError(t, hsmcallbacks.RegisterTaskSerializers(hsmReg))
	testShard.SetStateMachineRegistry(hsmReg)

	chasmReg := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, chasmReg.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
	require.NoError(t, chasmReg.Register(chasmcallback.NewNilLibrary()))
	testShard.SetChasmRegistry(chasmReg)

	cbCfg := test.NewCallbacksValidatorConfig()
	cbCfg.MaxCallbacksPerExecution = func(string) int { return 2000 }
	testShard.SetCallbackValidator(test.NewCallbacksValidator(t, cbCfg))

	nsEntry := tests.LocalNamespaceEntry
	nsID := nsEntry.ID()
	wfID := "target-wf-" + uuid.NewString()

	testShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(nsID).Return(nsEntry, nil).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()

	cache := newAPIWorkflowCache()
	env := &apiTestEnv{
		t:           t,
		ctrl:        ctrl,
		testShard:   testShard,
		cfg:         cfg,
		cache:       cache,
		checker:     api.NewWorkflowConsistencyChecker(testShard, cache),
		namespaceID: nsID,
		nsEntry:     nsEntry,
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
		events: make(map[string]map[int64]*historypb.HistoryEvent),
	}

	testShard.Resource.ExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).DoAndReturn(
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

	testShard.MockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).Do(
		func(key events.EventKey, event *historypb.HistoryEvent) {
			env.recordEvent(key.RunID, event)
		},
	).AnyTimes()
	testShard.MockEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ int32, key events.EventKey, _ int64, _ []byte) (*historypb.HistoryEvent, error) {
			env.mu.Lock()
			defer env.mu.Unlock()
			if runMap, ok := env.events[key.RunID]; ok {
				if ev, ok := runMap[key.EventID]; ok {
					return ev, nil
				}
			}
			return nil, serviceerror.NewNotFound("event not found")
		},
	).AnyTimes()

	return env
}

func (e *apiTestEnv) recordEvent(runID string, ev *historypb.HistoryEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.events[runID] == nil {
		e.events[runID] = make(map[int64]*historypb.HistoryEvent)
	}
	e.events[runID][ev.GetEventId()] = ev
}

func (e *apiTestEnv) setCurrentRun(runID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.defaultCurr = currentExecutionResponse{runID: runID}
}

func (e *apiTestEnv) setCurrentSequence(seq ...currentExecutionResponse) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.currentSeq = seq
	e.currentIdx = 0
}

func (e *apiTestEnv) setCallbackLimits(maxCount, maxSize int) {
	cbCfg := test.NewCallbacksValidatorConfig()
	cbCfg.MaxCallbacksPerExecution = func(string) int { return maxCount }
	if maxSize > 0 {
		cbCfg.TotalCallbacksMaxSize = func(string) int { return maxSize }
	}
	e.testShard.SetCallbackValidator(test.NewCallbacksValidator(e.t, cbCfg))
}

// addRealOpenRun creates a real started MutableStateImpl with a completed first WFT
// (ready for attachments, continue-as-new, or completion).
func (e *apiTestEnv) addRealOpenRun(runID string, extraCallbacks ...*commonpb.Callback) (*apiRun, *workflow.MutableStateImpl, *historypb.HistoryEvent) {
	e.t.Helper()
	ms := workflow.NewMutableState(
		e.testShard,
		e.testShard.MockEventsCache,
		e.testShard.GetLogger(),
		e.nsEntry,
		e.workflowID,
		runID,
		time.Now().UTC(),
	)
	tq := &taskqueuepb.TaskQueue{Name: "tq"}
	startEv, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: e.workflowID, RunId: runID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "start-" + runID,
				WorkflowType:        &commonpb.WorkflowType{Name: "wf-type"},
				TaskQueue:           tq,
				CompletionCallbacks: extraCallbacks,
			},
		},
	)
	require.NoError(e.t, err)
	e.recordEvent(runID, startEv)

	wft, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	require.NoError(e.t, err)
	_, wft, err = ms.AddWorkflowTaskStartedEvent(wft.ScheduledEventID, "", tq, "", nil, nil, nil, false, nil, 0, "")
	require.NoError(e.t, err)
	completedEv, err := ms.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		historyi.WorkflowTaskCompletionLimits{MaxResetPoints: 10, MaxSearchAttributeValueSize: 1024},
	)
	require.NoError(e.t, err)
	e.recordEvent(runID, completedEv)

	_, initBatches, err := ms.CloseTransactionAsSnapshot(e.t.Context(), historyi.TransactionPolicyActive)
	require.NoError(e.t, err)
	var initEvents []*historypb.HistoryEvent
	for _, b := range initBatches {
		initEvents = append(initEvents, b.Events...)
	}

	run := &apiRun{
		key:        definition.NewWorkflowKey(e.namespaceID.String(), e.workflowID, runID),
		ms:         ms,
		initEvents: initEvents,
	}
	e.cache.addRun(run)
	return run, ms, completedEv
}

// continueAsNewRun executes a real ContinueAsNew transition from prevMS onto a new run
// (inheriting callbacks with ResultSubscription intact) and registers the new run in cache.
func (e *apiTestEnv) continueAsNewRun(
	prevRunID string,
	prevMS *workflow.MutableStateImpl,
	wftCompletedEventID int64,
) (*apiRun, *workflow.MutableStateImpl, *historypb.HistoryEvent) {
	e.t.Helper()
	canEv, newMS, err := prevMS.AddContinueAsNewEvent(
		e.t.Context(),
		wftCompletedEventID,
		"",
		&commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
			WorkflowRunTimeout: prevMS.GetExecutionInfo().WorkflowRunTimeout,
		},
		nil,
	)
	require.NoError(e.t, err)
	e.recordEvent(prevRunID, canEv)
	_, _, err = prevMS.CloseTransactionAsMutation(e.t.Context(), historyi.TransactionPolicyActive)
	require.NoError(e.t, err)

	realNewMS, ok := newMS.(*workflow.MutableStateImpl)
	require.True(e.t, ok)
	newRunID := realNewMS.GetExecutionState().GetRunId()

	tq := &taskqueuepb.TaskQueue{Name: "tq"}
	wft, err := realNewMS.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	require.NoError(e.t, err)
	_, wft, err = realNewMS.AddWorkflowTaskStartedEvent(wft.ScheduledEventID, "", tq, "", nil, nil, nil, false, nil, 0, "")
	require.NoError(e.t, err)
	completedEv, err := realNewMS.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		historyi.WorkflowTaskCompletionLimits{MaxResetPoints: 10, MaxSearchAttributeValueSize: 1024},
	)
	require.NoError(e.t, err)
	e.recordEvent(newRunID, completedEv)
	_, _, err = realNewMS.CloseTransactionAsSnapshot(e.t.Context(), historyi.TransactionPolicyActive)
	require.NoError(e.t, err)

	run := &apiRun{
		key: definition.NewWorkflowKey(e.namespaceID.String(), e.workflowID, newRunID),
		ms:  realNewMS,
	}
	e.cache.addRun(run)
	e.setCurrentRun(newRunID)
	return run, realNewMS, completedEv
}

// completeRun closes a real open run with a Completed event and commits its mutation.
func (e *apiTestEnv) completeRun(
	runID string,
	ms *workflow.MutableStateImpl,
	wftCompletedEventID int64,
	payloadData []byte,
) *historypb.HistoryEvent {
	e.t.Helper()
	closeEv, err := ms.AddCompletedWorkflowEvent(
		wftCompletedEventID,
		&commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{{
					Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
					Data:     payloadData,
				}},
			},
		},
		"",
	)
	require.NoError(e.t, err)
	e.recordEvent(runID, closeEv)
	_, _, err = ms.CloseTransactionAsMutation(e.t.Context(), historyi.TransactionPolicyActive)
	require.NoError(e.t, err)
	return closeEv
}

func makeCallerCallback(t *testing.T, caller *historyservice.NexusCallerInfo, requestID string, chasmToken bool) *commonpb.Callback {
	t.Helper()
	gen := commonnexus.NewCallbackTokenGenerator()
	var completion *tokenspb.NexusOperationCompletion
	if chasmToken {
		refBytes, err := (&persistencespb.ChasmComponentRef{
			NamespaceId: caller.GetNamespaceId(),
			BusinessId:  caller.GetWorkflowId(),
			RunId:       caller.GetRunId(),
		}).Marshal()
		require.NoError(t, err)
		completion = &tokenspb.NexusOperationCompletion{
			ComponentRef: refBytes,
			RequestId:    requestID,
		}
	} else {
		completion = &tokenspb.NexusOperationCompletion{
			NamespaceId: caller.GetNamespaceId(),
			WorkflowId:  caller.GetWorkflowId(),
			RunId:       caller.GetRunId(),
			Ref:         &persistencespb.StateMachineRef{},
			RequestId:   requestID,
		}
	}
	tok, err := gen.Tokenize(completion)
	require.NoError(t, err)
	header := make(nexus.Header, 1)
	header.Set(commonnexus.CallbackTokenHeader, tok)
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    commonnexus.SystemCallbackURL,
				Header: header,
			},
		},
	}
}

func newCallerInfo(nsID namespace.ID, reqID string) *historyservice.NexusCallerInfo {
	return &historyservice.NexusCallerInfo{
		NamespaceId: nsID.String(),
		WorkflowId:  "caller-wf-" + uuid.NewString(),
		RunId:       uuid.NewString(),
		RequestId:   reqID,
	}
}

func TestStartValidation(t *testing.T) {
	t.Parallel()

	env := newAPITestEnv(t, true)
	validReqID := uuid.NewString()
	validCaller := newCallerInfo(env.namespaceID, validReqID)
	validCB := makeCallerCallback(t, validCaller, validReqID, true)
	otherCaller := newCallerInfo(env.namespaceID, validReqID)

	testCases := []struct {
		name string
		req  StartRequest
	}{
		{
			name: "empty namespace ID",
			req: StartRequest{
				NamespaceID: "",
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   validReqID,
				Callback:    validCB,
			},
		},
		{
			name: "empty request ID",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   "",
				Callback:    validCB,
			},
		},
		{
			name: "caller request ID mismatch",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   uuid.NewString(),
				Callback:    validCB,
			},
		},
		{
			name: "nil caller",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      nil,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   validReqID,
				Callback:    validCB,
			},
		},
		{
			name: "nil callback",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   validReqID,
				Callback:    nil,
			},
		},
		{
			name: "callback with caller-supplied result_subscription",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   validReqID,
				Callback: func() *commonpb.Callback {
					cb := common.CloneProto(validCB)
					cb.ResultSubscription = &commonpb.Callback_ResultSubscription{RequestId: validReqID}
					return cb
				}(),
			},
		},
		{
			name: "callback missing nexus URL",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   validReqID,
				Callback:    &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{}}},
			},
		},
		{
			name: "callback missing token header",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   validReqID,
				Callback: &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
					},
				},
			},
		},
		{
			name: "callback token belongs to different caller",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   validReqID,
				Callback:    makeCallerCallback(t, otherCaller, validReqID, true),
			},
		},
		{
			name: "callback token request ID mismatch",
			req: StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      validCaller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   validReqID,
				Callback:    makeCallerCallback(t, validCaller, uuid.NewString(), true),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Start(t.Context(), env.testShard, env.checker, tc.req)
			var invalidArg *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArg)
			require.False(t, api.IsRetryableError(err))
			require.Zero(t, env.cache.totalUpdateCalls())
			env.cache.requireNoLeakedLocks(t)
		})
	}

	t.Run("passive namespace returns NamespaceNotActive without acquiring locks", func(t *testing.T) {
		t.Parallel()
		passiveEnv := newAPITestEnv(t, true)
		passiveNS := tests.GlobalStandbyNamespaceEntry
		passiveEnv.testShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(passiveNS.ID()).Return(passiveNS, nil)
		reqID := uuid.NewString()
		caller := newCallerInfo(passiveNS.ID(), reqID)
		_, err := Start(t.Context(), passiveEnv.testShard, passiveEnv.checker, StartRequest{
			NamespaceID: passiveNS.ID(),
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: passiveEnv.workflowID},
			RequestID:   reqID,
			Callback:    makeCallerCallback(t, caller, reqID, true),
		})
		var nsNotActive *serviceerror.NamespaceNotActive
		require.ErrorAs(t, err, &nsNotActive)
		require.Zero(t, passiveEnv.cache.totalUpdateCalls())
		passiveEnv.cache.requireNoLeakedLocks(t)
	})
}

func TestStartFreshOpenAttachAndClosedRead(t *testing.T) {
	t.Parallel()

	for _, mode := range []struct {
		name           string
		chasmCallbacks bool
		chasmToken     bool
	}{
		{name: "CHASM callbacks with CHASM caller token", chasmCallbacks: true, chasmToken: true},
		{name: "CHASM callbacks with HSM caller token", chasmCallbacks: true, chasmToken: false},
		{name: "HSM callbacks with HSM caller token", chasmCallbacks: false, chasmToken: false},
	} {
		t.Run(mode.name, func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, mode.chasmCallbacks)
			runID := uuid.NewString()
			run, ms, _ := env.addRealOpenRun(runID)
			env.setCurrentRun(runID)

			reqID := uuid.NewString()
			caller := newCallerInfo(env.namespaceID, reqID)
			inputCB := makeCallerCallback(t, caller, reqID, mode.chasmToken)
			inputSnapshot := common.CloneProto(inputCB)

			got, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqID,
				Callback:    inputCB,
			})
			require.NoError(t, err)
			require.Nil(t, got.Closed)
			require.NotNil(t, got.Subscribed)
			require.Equal(t, 1, run.updateCalls)

			// Input callback must not have been mutated in place.
			protorequire.ProtoEqual(t, inputSnapshot, inputCB)
			require.Nil(t, inputCB.GetResultSubscription())

			// Token decodes to version 1 with exact chain and request metadata.
			tok, err := decodeResultSubscriptionToken(got.Subscribed.Token)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &tokenspb.ResultSubscriptionToken{
				Version:       1,
				NamespaceId:   env.namespaceID.String(),
				WorkflowId:    env.workflowID,
				FirstRunId:    runID,
				ResolvedRunId: runID,
				RequestId:     reqID,
			}, tok)

			// Link references the WorkflowExecutionOptionsUpdated event by request ID.
			wantLink := api.GenerateRequestIDRefLink(
				env.nsEntry.Name().String(),
				env.workflowID,
				runID,
				reqID,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			)
			protorequire.ProtoEqual(t, wantLink, got.Subscribed.Link)

			// Exactly one WorkflowExecutionOptionsUpdated event committed with ResultSubscription.
			require.Len(t, run.committedEvents, 1)
			ev := run.committedEvents[0]
			require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, ev.GetEventType())
			require.True(t, ev.GetWorkerMayIgnore())
			attrs := ev.GetWorkflowExecutionOptionsUpdatedEventAttributes()
			require.Equal(t, reqID, attrs.GetAttachedRequestId())
			require.Len(t, attrs.GetAttachedCompletionCallbacks(), 1)
			protorequire.ProtoEqual(t, &commonpb.Callback_ResultSubscription{
				RequestId:           reqID,
				ResultFormatVersion: 1,
				ResolvedExecution: &commonpb.WorkflowExecution{
					WorkflowId: env.workflowID,
					RunId:      runID,
				},
			}, attrs.GetAttachedCompletionCallbacks()[0].GetResultSubscription())

			// Stored callback exists in mutable state.
			stored, found, err := findResultSubscription(t.Context(), ms, reqID)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, stored.cancelable)

			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})
	}

	t.Run("explicit superseded run attaches on current same-chain run with FirstRunId from chain head", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		c1ID := uuid.NewString()
		_, c1MS, c1WFT := env.addRealOpenRun(c1ID)
		env.setCurrentRun(c1ID)
		c2Run, _, _ := env.continueAsNewRun(c1ID, c1MS, c1WFT.GetEventId())

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		got, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1ID},
			RequestID:   reqID,
			Callback:    makeCallerCallback(t, caller, reqID, true),
		})
		require.NoError(t, err)
		require.NotNil(t, got.Subscribed)
		require.Equal(t, 1, c2Run.updateCalls)

		tok, err := decodeResultSubscriptionToken(got.Subscribed.Token)
		require.NoError(t, err)
		require.Equal(t, c1ID, tok.GetFirstRunId())
		require.Equal(t, c2Run.key.RunID, tok.GetResolvedRunId())
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("internal CHASM callback URL attaches and cancels cleanly", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, ms, _ := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		refBytes, err := (&persistencespb.ChasmComponentRef{
			NamespaceId: caller.GetNamespaceId(),
			BusinessId:  caller.GetWorkflowId(),
			RunId:       caller.GetRunId(),
		}).Marshal()
		require.NoError(t, err)
		cb, err := chasm.GenerateNexusCallback(refBytes, reqID, true)
		require.NoError(t, err)

		got, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    cb,
		})
		require.NoError(t, err)
		require.NotNil(t, got.Subscribed)
		require.Equal(t, 1, run.updateCalls)

		require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, got.Subscribed.Token))
		require.Equal(t, 2, run.updateCalls)
		stored, ok, err := findResultSubscription(t.Context(), ms, reqID)
		require.NoError(t, err)
		require.True(t, ok)
		require.False(t, stored.cancelable)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("fresh closed read writes nothing when flag is on and rejects when flag is off", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, ms, wftCompleted := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)
		closeEv := env.completeRun(runID, ms, wftCompleted.GetEventId(), []byte("done-result"))

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		got, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    makeCallerCallback(t, caller, reqID, true),
		})
		require.NoError(t, err)
		require.Nil(t, got.Subscribed)
		protorequire.ProtoEqual(t, closeEv, got.Closed)
		require.Zero(t, run.updateCalls)

		// Disable flag: a fresh closed read with a different request ID must be rejected without writes.
		env.cfg.EnableGetWorkflowExecutionResultFromWorkflow = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
		reqID2 := uuid.NewString()
		caller2 := newCallerInfo(env.namespaceID, reqID2)
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller2,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID2,
			Callback:    makeCallerCallback(t, caller2, reqID2, true),
		})
		var failedPrecond *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecond)
		require.Zero(t, run.updateCalls)
		env.cache.requireNoLeakedLocks(t)
	})
}

func TestStartDedupFlagCapInheritedAndLostResponse(t *testing.T) {
	t.Parallel()

	t.Run("duplicate after disablement and cap exhaustion returns original token without writes", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, _, _ := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		cb := makeCallerCallback(t, caller, reqID, true)

		first, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    cb,
		})
		require.NoError(t, err)
		require.Equal(t, 1, run.updateCalls)

		// Disable feature flag and drop callback cap to 0.
		env.cfg.EnableGetWorkflowExecutionResultFromWorkflow = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
		env.setCallbackLimits(0, 1)

		// Fresh waiter is rejected with FailedPrecondition (flag off) and zero writes.
		reqID2 := uuid.NewString()
		caller2 := newCallerInfo(env.namespaceID, reqID2)
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller2,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID2,
			Callback:    makeCallerCallback(t, caller2, reqID2, true),
		})
		var failedPrecond *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecond)
		require.Equal(t, 1, run.updateCalls)

		// Duplicate of original waiter succeeds and returns identical token and link with zero writes.
		dup, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    cb,
		})
		require.NoError(t, err)
		require.Equal(t, first.Subscribed.Token, dup.Subscribed.Token)
		protorequire.ProtoEqual(t, first.Subscribed.Link, dup.Subscribed.Link)
		require.Equal(t, 1, run.updateCalls)
		env.cache.requireNoLeakedLocks(t)
	})

	for _, chasmMode := range []bool{true, false} {
		name := "inherited subscription after CAN returns original resolved-run token (HSM)"
		if chasmMode {
			name = "inherited subscription after CAN returns original resolved-run token (CHASM)"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			c1ID := uuid.NewString()
			c1Run, c1MS, c1WFT := env.addRealOpenRun(c1ID)
			env.setCurrentRun(c1ID)

			reqID := uuid.NewString()
			caller := newCallerInfo(env.namespaceID, reqID)
			cb := makeCallerCallback(t, caller, reqID, chasmMode)

			first, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqID,
				Callback:    cb,
			})
			require.NoError(t, err)
			require.Equal(t, 1, c1Run.updateCalls)

			// Continue-as-new to C2; callbacks are re-keyed under C2's start request ID.
			c2Run, c2MS, c2WFT := env.continueAsNewRun(c1ID, c1MS, c1WFT.GetEventId())

			// Disable flag and drop callback caps to 0.
			env.cfg.EnableGetWorkflowExecutionResultFromWorkflow = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
			env.cfg.MaxCallbacksPerWorkflow = dynamicconfig.GetIntPropertyFnFilteredByNamespace(0)
			env.setCallbackLimits(0, 1)

			// Retry against open C2 (both unpinned and pinned to C1) returns C1's original token.
			for _, targetRunID := range []string{"", c1ID} {
				dup, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
					NamespaceID: env.namespaceID,
					Caller:      caller,
					Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: targetRunID},
					RequestID:   reqID,
					Callback:    cb,
				})
				require.NoError(t, err)
				require.Nil(t, dup.Closed)
				require.Equal(t, first.Subscribed.Token, dup.Subscribed.Token)
				protorequire.ProtoEqual(t, first.Subscribed.Link, dup.Subscribed.Link)
				require.Zero(t, c2Run.updateCalls)
			}

			// Lost response after close: close C2 (callback moves to SCHEDULED), retry still returns original token.
			env.completeRun(c2Run.key.RunID, c2MS, c2WFT.GetEventId(), []byte("c2-result"))
			afterClose, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: c1ID},
				RequestID:   reqID,
				Callback:    cb,
			})
			require.NoError(t, err)
			require.Nil(t, afterClose.Closed)
			require.Equal(t, first.Subscribed.Token, afterClose.Subscribed.Token)
			protorequire.ProtoEqual(t, first.Subscribed.Link, afterClose.Subscribed.Link)
			require.Zero(t, c2Run.updateCalls)

			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})
	}

	t.Run("collision with different caller or non-result callback rejects without writes", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, ms, _ := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		// Attach a result subscription for caller1 under req1.
		req1 := uuid.NewString()
		caller1 := newCallerInfo(env.namespaceID, req1)
		_, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller1,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req1,
			Callback:    makeCallerCallback(t, caller1, req1, true),
		})
		require.NoError(t, err)

		// Different caller attempting Start with req1 is rejected without writes.
		caller2 := newCallerInfo(env.namespaceID, req1)
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller2,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req1,
			Callback:    makeCallerCallback(t, caller2, req1, true),
		})
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
		require.Equal(t, 1, run.updateCalls)

		// Attach a legacy (non-result) callback under legacyReqID.
		legacyReqID := uuid.NewString()
		_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(
			nil, false, legacyReqID,
			[]*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: "http://legacy.example/cb"},
				},
			}},
			nil, "", nil, nil, false, nil,
		)
		require.NoError(t, err)
		_, _, err = ms.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyActive)
		require.NoError(t, err)

		// Start with legacyReqID must not silently no-op or treat the legacy callback as a result subscription.
		caller3 := newCallerInfo(env.namespaceID, legacyReqID)
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller3,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   legacyReqID,
			Callback:    makeCallerCallback(t, caller3, legacyReqID, true),
		})
		require.ErrorAs(t, err, &invalidArg)
		require.Equal(t, 1, run.updateCalls)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("new unrelated chain between unpinned attempts is deduplication boundary", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		c1ID := uuid.NewString()
		_, c1MS, c1WFT := env.addRealOpenRun(c1ID)
		env.setCurrentRun(c1ID)

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		cb := makeCallerCallback(t, caller, reqID, true)

		first, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    cb,
		})
		require.NoError(t, err)

		// C1 closes, then an unrelated chain R2 reuses the same workflow ID.
		env.completeRun(c1ID, c1MS, c1WFT.GetEventId(), []byte("c1-done"))
		r2ID := uuid.NewString()
		r2Run, _, _ := env.addRealOpenRun(r2ID)
		env.setCurrentRun(r2ID)

		// Unpinned retry attaches freshly to R2 with R2's chain identity.
		second, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    cb,
		})
		require.NoError(t, err)
		require.NotEqual(t, first.Subscribed.Token, second.Subscribed.Token)
		tok2, err := decodeResultSubscriptionToken(second.Subscribed.Token)
		require.NoError(t, err)
		require.Equal(t, r2ID, tok2.GetFirstRunId())
		require.Equal(t, r2ID, tok2.GetResolvedRunId())
		require.Equal(t, 1, r2Run.updateCalls)
		env.cache.requireNoLeakedLocks(t)
	})
}

func TestStartCapacityInFlightUpdatesAndHistoryBudget(t *testing.T) {
	t.Parallel()

	t.Run("CHASM callback count cap and in-flight Update reservations return ResourceExhausted with no writes", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		env.setCallbackLimits(2, 0)
		runID := uuid.NewString()
		run, _, _ := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		// Attach 1st waiter (1 of 2 slots used).
		req1 := uuid.NewString()
		caller1 := newCallerInfo(env.namespaceID, req1)
		_, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller1,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req1,
			Callback:    makeCallerCallback(t, caller1, req1, true),
		})
		require.NoError(t, err)
		require.Equal(t, 1, run.updateCalls)

		// Reserve the 2nd slot with an in-flight Update callback.
		run.updateReg.setInFlight(chasmworkflow.CallbackAddition{
			UpdateID:  "upd-1",
			RequestID: "upd-req-1",
			Callbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: "http://update.example/cb"},
				},
			}},
		})

		// 2nd waiter must be rejected with ResourceExhausted without any write.
		req2 := uuid.NewString()
		caller2 := newCallerInfo(env.namespaceID, req2)
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller2,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req2,
			Callback:    makeCallerCallback(t, caller2, req2, true),
		})
		var resExhausted *serviceerror.ResourceExhausted
		require.ErrorAs(t, err, &resExhausted)
		require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, resExhausted.Cause)
		require.Equal(t, 1, run.updateCalls)

		// Once the in-flight Update clears, 2nd waiter claims the last free slot.
		run.updateReg.setInFlight()
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller2,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req2,
			Callback:    makeCallerCallback(t, caller2, req2, true),
		})
		require.NoError(t, err)
		require.Equal(t, 2, run.updateCalls)

		// 3rd waiter exceeds cap -> ResourceExhausted, zero writes.
		req3 := uuid.NewString()
		caller3 := newCallerInfo(env.namespaceID, req3)
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller3,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req3,
			Callback:    makeCallerCallback(t, caller3, req3, true),
		})
		require.ErrorAs(t, err, &resExhausted)
		require.Equal(t, 2, run.updateCalls)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("CHASM total callback size cap returns ResourceExhausted with no writes", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, _, _ := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		env.setCallbackLimits(2000, 10)
		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		_, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    makeCallerCallback(t, caller, reqID, true),
		})
		var resExhausted *serviceerror.ResourceExhausted
		require.ErrorAs(t, err, &resExhausted)
		require.Zero(t, run.updateCalls)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("HSM callback cap returns ResourceExhausted with no writes", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, false)
		env.cfg.MaxCallbacksPerWorkflow = dynamicconfig.GetIntPropertyFnFilteredByNamespace(1)
		runID := uuid.NewString()
		run, _, _ := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		req1 := uuid.NewString()
		caller1 := newCallerInfo(env.namespaceID, req1)
		_, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller1,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req1,
			Callback:    makeCallerCallback(t, caller1, req1, false),
		})
		require.NoError(t, err)
		require.Equal(t, 1, run.updateCalls)

		req2 := uuid.NewString()
		caller2 := newCallerInfo(env.namespaceID, req2)
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller2,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req2,
			Callback:    makeCallerCallback(t, caller2, req2, false),
		})
		var resExhausted *serviceerror.ResourceExhausted
		require.ErrorAs(t, err, &resExhausted)
		require.Equal(t, 1, run.updateCalls)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("target history budget limits return ResourceExhausted with no writes", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name      string
			configure func(env *apiTestEnv, ms *workflow.MutableStateImpl)
		}{
			{
				name: "history event count limit",
				configure: func(env *apiTestEnv, ms *workflow.MutableStateImpl) {
					limit := int(ms.GetNextEventID() - 1)
					env.cfg.HistoryCountLimitError = dynamicconfig.GetIntPropertyFnFilteredByNamespace(limit)
				},
			},
			{
				name: "history event count limit with buffered event",
				configure: func(env *apiTestEnv, ms *workflow.MutableStateImpl) {
					wft, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
					require.NoError(env.t, err)
					_, _, err = ms.AddWorkflowTaskStartedEvent(wft.ScheduledEventID, "", &taskqueuepb.TaskQueue{Name: "tq"}, "", nil, nil, nil, false, nil, 0, "")
					require.NoError(env.t, err)
					_, err = ms.AddWorkflowExecutionSignaled("sig", nil, "id", nil, "sig-req", nil)
					require.NoError(env.t, err)
					require.Equal(env.t, 1, ms.NumBufferedEvents())
					limit := int(ms.GetNextEventID())
					env.cfg.HistoryCountLimitError = dynamicconfig.GetIntPropertyFnFilteredByNamespace(limit)
				},
			},
			{
				name: "history size limit",
				configure: func(env *apiTestEnv, ms *workflow.MutableStateImpl) {
					ms.AddHistorySize(1000)
					env.cfg.HistorySizeLimitError = dynamicconfig.GetIntPropertyFnFilteredByNamespace(1010)
				},
			},
			{
				name: "mutable state size limit",
				configure: func(env *apiTestEnv, ms *workflow.MutableStateImpl) {
					env.cfg.MutableStateSizeLimitError = dynamicconfig.GetIntPropertyFn(ms.GetApproximatePersistedSize() + 10)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				env := newAPITestEnv(t, true)
				runID := uuid.NewString()
				run, ms, _ := env.addRealOpenRun(runID)
				env.setCurrentRun(runID)
				tc.configure(env, ms)

				reqID := uuid.NewString()
				caller := newCallerInfo(env.namespaceID, reqID)
				_, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
					NamespaceID: env.namespaceID,
					Caller:      caller,
					Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
					RequestID:   reqID,
					Callback:    makeCallerCallback(t, caller, reqID, true),
				})
				var resExhausted *serviceerror.ResourceExhausted
				require.ErrorAs(t, err, &resExhausted)
				require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT, resExhausted.Cause)
				require.Zero(t, run.updateCalls)
				env.cache.requireNoLeakedLocks(t)
			})
		}
	})
}

func TestStartCloseRacingAttachBothLockOrders(t *testing.T) {
	t.Parallel()

	t.Run("close acquires lease first then Start returns Closed without writes", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, ms, wftCompleted := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		closeAcquired := make(chan struct{})
		releaseClose := make(chan struct{})
		var closeEv *historypb.HistoryEvent

		// Acquire workflow lock for close before starting Start.
		closeDone := make(chan error, 1)
		go func() {
			_, releaseFn, err := env.cache.GetOrCreateWorkflowExecution(
				t.Context(),
				env.testShard,
				env.namespaceID,
				&commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: runID},
				locks.PriorityHigh,
			)
			if err != nil {
				closeDone <- err
				return
			}
			close(closeAcquired)
			<-releaseClose
			closeEv = env.completeRun(runID, ms, wftCompleted.GetEventId(), []byte("closed-first"))
			releaseFn(nil)
			closeDone <- nil
		}()

		<-closeAcquired

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		cb := makeCallerCallback(t, caller, reqID, true)
		type startOut struct {
			res StartResult
			err error
		}
		startDone := make(chan startOut, 1)
		go func() {
			res, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqID,
				Callback:    cb,
			})
			startDone <- startOut{res: res, err: err}
		}()

		// Release close so it commits and unlocks; Start then observes the closed execution.
		close(releaseClose)
		require.NoError(t, <-closeDone)

		out := <-startDone
		require.NoError(t, out.err)
		require.Nil(t, out.res.Subscribed)
		protorequire.ProtoEqual(t, closeEv, out.res.Closed)
		require.Zero(t, run.updateCalls)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("Start acquires lease first attaches callback then Close schedules it", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, ms, wftCompleted := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		startInUpdate := make(chan struct{})
		releaseStart := make(chan struct{})
		run.updateFn = func(ctx context.Context, ms historyi.MutableState) error {
			close(startInUpdate)
			<-releaseStart
			realMS := ms.(*workflow.MutableStateImpl)
			_, batches, err := realMS.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
			if err != nil {
				return err
			}
			run.mu.Lock()
			for _, b := range batches {
				run.committedEvents = append(run.committedEvents, b.Events...)
			}
			run.mu.Unlock()
			return nil
		}

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		cb := makeCallerCallback(t, caller, reqID, true)
		type startOut struct {
			res StartResult
			err error
		}
		startDone := make(chan startOut, 1)
		go func() {
			res, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqID,
				Callback:    cb,
			})
			startDone <- startOut{res: res, err: err}
		}()

		<-startInUpdate

		closeDone := make(chan error, 1)
		go func() {
			_, releaseFn, err := env.cache.GetOrCreateWorkflowExecution(
				t.Context(),
				env.testShard,
				env.namespaceID,
				&commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: runID},
				locks.PriorityHigh,
			)
			if err != nil {
				closeDone <- err
				return
			}
			env.completeRun(runID, ms, wftCompleted.GetEventId(), []byte("closed-second"))
			releaseFn(nil)
			closeDone <- nil
		}()

		close(releaseStart)

		out := <-startDone
		require.NoError(t, out.err)
		require.NotNil(t, out.res.Subscribed)
		require.NoError(t, <-closeDone)

		wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
		require.NoError(t, err)
		storedCB, ok := wf.FindResultSubscription(ctx, reqID)
		require.True(t, ok)
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, storedCB.Status)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("stale cache conflict when Close commits concurrently retries and returns Closed", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		staleRun, staleMS, _ := env.addRealOpenRun(runID)

		// Build a fresh closed version of the same run to return after cache clear.
		freshMS := workflow.NewMutableState(
			env.testShard,
			env.testShard.MockEventsCache,
			env.testShard.GetLogger(),
			env.nsEntry,
			env.workflowID,
			runID,
			time.Now().UTC(),
		)
		tq := &taskqueuepb.TaskQueue{Name: "tq"}
		startEv, err := freshMS.AddWorkflowExecutionStartedEvent(
			&commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: runID},
			&historyservice.StartWorkflowExecutionRequest{
				StartRequest: &workflowservice.StartWorkflowExecutionRequest{
					RequestId:    "start-" + runID,
					WorkflowType: &commonpb.WorkflowType{Name: "wf-type"},
					TaskQueue:    tq,
				},
			},
		)
		require.NoError(t, err)
		env.recordEvent(runID, startEv)
		wft, err := freshMS.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
		require.NoError(t, err)
		_, wft, err = freshMS.AddWorkflowTaskStartedEvent(wft.ScheduledEventID, "", tq, "", nil, nil, nil, false, nil, 0, "")
		require.NoError(t, err)
		wftCompleted, err := freshMS.AddWorkflowTaskCompletedEvent(
			wft,
			&workflowservice.RespondWorkflowTaskCompletedRequest{},
			historyi.WorkflowTaskCompletionLimits{MaxResetPoints: 10, MaxSearchAttributeValueSize: 1024},
		)
		require.NoError(t, err)
		closeEv := env.completeRun(runID, freshMS, wftCompleted.GetEventId(), []byte("concurrent-close"))

		var cleared atomic.Bool
		staleRun.loadFn = func(context.Context) (historyi.MutableState, error) {
			if cleared.Load() {
				return freshMS, nil
			}
			return staleMS, nil
		}
		staleRun.clearFn = func() { cleared.Store(true) }
		staleRun.updateFn = func(context.Context, historyi.MutableState) error {
			return &persistence.CurrentWorkflowConditionFailedError{Msg: "run closed concurrently", RunID: runID}
		}
		env.setCurrentRun(runID)

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		got, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    makeCallerCallback(t, caller, reqID, true),
		})
		require.NoError(t, err)
		require.True(t, cleared.Load())
		require.Nil(t, got.Subscribed)
		protorequire.ProtoEqual(t, closeEv, got.Closed)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})
}

func TestStartConcurrentDuplicateAndLastFreeSlot(t *testing.T) {
	t.Parallel()

	t.Run("concurrent duplicate starts attach once and return identical token", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, ms, _ := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		cb := makeCallerCallback(t, caller, reqID, true)

		const workers = 16
		type out struct {
			res StartResult
			err error
		}
		results := make(chan out, workers)
		startGate := make(chan struct{})
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				<-startGate
				res, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
					NamespaceID: env.namespaceID,
					Caller:      caller,
					Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
					RequestID:   reqID,
					Callback:    common.CloneProto(cb),
				})
				results <- out{res: res, err: err}
			})
		}
		close(startGate)
		wg.Wait()
		close(results)

		var expectedToken string
		var expectedLink *commonpb.Link
		for r := range results {
			require.NoError(t, r.err)
			require.NotNil(t, r.res.Subscribed)
			if expectedToken == "" {
				expectedToken = r.res.Subscribed.Token
				expectedLink = r.res.Subscribed.Link
			} else {
				require.Equal(t, expectedToken, r.res.Subscribed.Token)
				protorequire.ProtoEqual(t, expectedLink, r.res.Subscribed.Link)
			}
		}
		require.Equal(t, 1, run.updateCalls)
		wf, _, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
		require.NoError(t, err)
		require.Len(t, wf.Callbacks, 1)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("concurrent distinct waiters competing for last free slot admit exactly one", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		env.setCallbackLimits(2, 0)
		runID := uuid.NewString()
		run, ms, _ := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		// Occupy 1 of 2 slots first.
		seedReq := uuid.NewString()
		seedCaller := newCallerInfo(env.namespaceID, seedReq)
		_, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      seedCaller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   seedReq,
			Callback:    makeCallerCallback(t, seedCaller, seedReq, true),
		})
		require.NoError(t, err)
		require.Equal(t, 1, run.updateCalls)

		const contenders = 8
		type out struct {
			res StartResult
			err error
		}
		results := make(chan out, contenders)
		startGate := make(chan struct{})
		var wg sync.WaitGroup
		for range contenders {
			reqID := uuid.NewString()
			caller := newCallerInfo(env.namespaceID, reqID)
			cb := makeCallerCallback(t, caller, reqID, true)
			wg.Go(func() {
				<-startGate
				res, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
					NamespaceID: env.namespaceID,
					Caller:      caller,
					Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
					RequestID:   reqID,
					Callback:    cb,
				})
				results <- out{res: res, err: err}
			})
		}
		close(startGate)
		wg.Wait()
		close(results)

		succeeded := 0
		rejected := 0
		for r := range results {
			if r.err == nil {
				require.NotNil(t, r.res.Subscribed)
				succeeded++
			} else {
				var resExhausted *serviceerror.ResourceExhausted
				require.ErrorAs(t, r.err, &resExhausted)
				rejected++
			}
		}
		require.Equal(t, 1, succeeded)
		require.Equal(t, contenders-1, rejected)
		require.Equal(t, 2, run.updateCalls)

		wf, _, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
		require.NoError(t, err)
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int64(2), wf.GetCallbackMetadata().GetTotalCallbacksCount())
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})
}

func TestCancelTokenAndCallerValidation(t *testing.T) {
	t.Parallel()

	env := newAPITestEnv(t, true)
	runID := uuid.NewString()
	run, ms, _ := env.addRealOpenRun(runID)
	env.setCurrentRun(runID)

	reqID := uuid.NewString()
	caller := newCallerInfo(env.namespaceID, reqID)
	sub, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
		NamespaceID: env.namespaceID,
		Caller:      caller,
		Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
		RequestID:   reqID,
		Callback:    makeCallerCallback(t, caller, reqID, true),
	})
	require.NoError(t, err)
	require.Equal(t, 1, run.updateCalls)

	marshalToken := func(tok *tokenspb.ResultSubscriptionToken) string {
		b, err := proto.Marshal(tok)
		require.NoError(t, err)
		return base64.RawURLEncoding.EncodeToString(b)
	}

	invalidCases := []struct {
		name        string
		namespaceID namespace.ID
		caller      *historyservice.NexusCallerInfo
		token       string
	}{
		{
			name:        "empty token",
			namespaceID: env.namespaceID,
			caller:      caller,
			token:       "",
		},
		{
			name:        "invalid base64",
			namespaceID: env.namespaceID,
			caller:      caller,
			token:       "%%%not-base64%%%",
		},
		{
			name:        "invalid protobuf bytes",
			namespaceID: env.namespaceID,
			caller:      caller,
			token:       base64.RawURLEncoding.EncodeToString([]byte{0xff, 0xff, 0xff}),
		},
		{
			name:        "unknown token version 0",
			namespaceID: env.namespaceID,
			caller:      caller,
			token: marshalToken(&tokenspb.ResultSubscriptionToken{
				Version:       0,
				NamespaceId:   env.namespaceID.String(),
				WorkflowId:    env.workflowID,
				FirstRunId:    runID,
				ResolvedRunId: runID,
				RequestId:     reqID,
			}),
		},
		{
			name:        "unknown token version 2",
			namespaceID: env.namespaceID,
			caller:      caller,
			token: marshalToken(&tokenspb.ResultSubscriptionToken{
				Version:       2,
				NamespaceId:   env.namespaceID.String(),
				WorkflowId:    env.workflowID,
				FirstRunId:    runID,
				ResolvedRunId: runID,
				RequestId:     reqID,
			}),
		},
		{
			name:        "token namespace mismatch",
			namespaceID: env.namespaceID,
			caller:      caller,
			token: marshalToken(&tokenspb.ResultSubscriptionToken{
				Version:       1,
				NamespaceId:   uuid.NewString(),
				WorkflowId:    env.workflowID,
				FirstRunId:    runID,
				ResolvedRunId: runID,
				RequestId:     reqID,
			}),
		},
		{
			name:        "nil caller",
			namespaceID: env.namespaceID,
			caller:      nil,
			token:       sub.Subscribed.Token,
		},
		{
			name:        "caller request ID mismatch",
			namespaceID: env.namespaceID,
			caller: &historyservice.NexusCallerInfo{
				NamespaceId: env.namespaceID.String(),
				WorkflowId:  caller.WorkflowId,
				RunId:       caller.RunId,
				RequestId:   uuid.NewString(),
			},
			token: sub.Subscribed.Token,
		},
		{
			name:        "different caller workflow ID",
			namespaceID: env.namespaceID,
			caller:      newCallerInfo(env.namespaceID, reqID),
			token:       sub.Subscribed.Token,
		},
		{
			name:        "different caller run ID",
			namespaceID: env.namespaceID,
			caller: &historyservice.NexusCallerInfo{
				NamespaceId: env.namespaceID.String(),
				WorkflowId:  caller.WorkflowId,
				RunId:       uuid.NewString(),
				RequestId:   reqID,
			},
			token: sub.Subscribed.Token,
		},
		{
			name:        "tampered resolved run ID in token",
			namespaceID: env.namespaceID,
			caller:      caller,
			token: marshalToken(&tokenspb.ResultSubscriptionToken{
				Version:       1,
				NamespaceId:   env.namespaceID.String(),
				WorkflowId:    env.workflowID,
				FirstRunId:    runID,
				ResolvedRunId: uuid.NewString(),
				RequestId:     reqID,
			}),
		},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			err := Cancel(t.Context(), env.testShard, env.checker, tc.namespaceID, tc.caller, tc.token)
			var invalidArg *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArg)
			require.False(t, api.IsRetryableError(err))
		})
	}

	// None of the invalid attempts mutated the subscription or wrote to persistence.
	require.Equal(t, 1, run.updateCalls)
	wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
	require.NoError(t, err)
	cb, ok := wf.FindResultSubscription(ctx, reqID)
	require.True(t, ok)
	require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, cb.Status)
	env.cache.requireNoLeakedLocks(t)
}

func TestCancelLifecycleChainResolutionAndWorkflowIDReuse(t *testing.T) {
	t.Parallel()

	for _, chasmMode := range []bool{true, false} {
		modeName := "HSM"
		if chasmMode {
			modeName = "CHASM"
		}
		t.Run(modeName+"_open_cancel_two_waiters_flag_off_and_idempotency", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			runID := uuid.NewString()
			run, ms, wftCompleted := env.addRealOpenRun(runID)
			env.setCurrentRun(runID)

			req1, req2 := uuid.NewString(), uuid.NewString()
			caller1, caller2 := newCallerInfo(env.namespaceID, req1), newCallerInfo(env.namespaceID, req2)

			sub1, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller1,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req1,
				Callback:    makeCallerCallback(t, caller1, req1, chasmMode),
			})
			require.NoError(t, err)
			_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller2,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req2,
				Callback:    makeCallerCallback(t, caller2, req2, chasmMode),
			})
			require.NoError(t, err)
			require.Equal(t, 2, run.updateCalls)

			// Cancel must continue working even when the feature flag is disabled.
			env.cfg.EnableGetWorkflowExecutionResultFromWorkflow = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token))
			require.Equal(t, 3, run.updateCalls)

			// Verify DEC-1 cancel event was written.
			lastEv := run.committedEvents[len(run.committedEvents)-1]
			require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, lastEv.GetEventType())
			require.True(t, lastEv.GetWorkerMayIgnore())
			require.Equal(t, []string{req1}, lastEv.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetCanceledResultSubscriptionRequestIds())

			// Waiter 1 is canceled, waiter 2 stays cancelable (STANDBY).
			s1, ok, err := findResultSubscription(t.Context(), ms, req1)
			require.NoError(t, err)
			require.True(t, ok)
			require.False(t, s1.cancelable)

			s2, ok, err := findResultSubscription(t.Context(), ms, req2)
			require.NoError(t, err)
			require.True(t, ok)
			require.True(t, s2.cancelable)

			// Second Cancel is a no-op with zero writes.
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token))
			require.Equal(t, 3, run.updateCalls)

			// Closing the workflow succeeds, leaving waiter 1 FAILED-canceled and scheduling waiter 2.
			env.completeRun(runID, ms, wftCompleted.GetEventId(), []byte("done"))
			if chasmMode {
				wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
				require.NoError(t, err)
				cb1, ok := wf.FindResultSubscription(ctx, req1)
				require.True(t, ok)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb1.Status)
				require.NotNil(t, cb1.GetLastAttemptFailure().GetCanceledFailureInfo())
				cb2, ok := wf.FindResultSubscription(ctx, req2)
				require.True(t, ok)
				require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, cb2.Status)
			} else {
				coll := hsmcallbacks.MachineCollection(ms.HSM())
				cb1, err := coll.Data("cb-" + req1 + "-0")
				require.NoError(t, err)
				require.Equal(t, enumsspb.CALLBACK_STATE_FAILED, cb1.State())
				require.NotNil(t, cb1.GetLastAttemptFailure().GetCanceledFailureInfo())
				cb2, err := coll.Data("cb-" + req2 + "-0")
				require.NoError(t, err)
				require.Equal(t, enumsspb.CALLBACK_STATE_SCHEDULED, cb2.State())
			}
			env.cache.requireNoLeakedLocks(t)
		})

		t.Run(modeName+"_cancel_moved_callback_on_current_same_chain_run", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			c1ID := uuid.NewString()
			c1Run, c1MS, c1WFT := env.addRealOpenRun(c1ID)
			env.setCurrentRun(c1ID)

			reqID := uuid.NewString()
			caller := newCallerInfo(env.namespaceID, reqID)
			sub, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqID,
				Callback:    makeCallerCallback(t, caller, reqID, chasmMode),
			})
			require.NoError(t, err)
			c1AcquiresBeforeCancel := c1Run.acquireCalls

			// Move chain C1 -> C2 via ContinueAsNew.
			c2Run, c2MS, _ := env.continueAsNewRun(c1ID, c1MS, c1WFT.GetEventId())

			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, sub.Subscribed.Token))
			require.Equal(t, c1AcquiresBeforeCancel, c1Run.acquireCalls, "Cancel should resolve current same-chain run directly without locking C1")
			require.Equal(t, 1, c2Run.updateCalls)

			stored, ok, err := findResultSubscription(t.Context(), c2MS, reqID)
			require.NoError(t, err)
			require.True(t, ok)
			require.False(t, stored.cancelable)
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})
	}

	t.Run("cancel never touches unrelated workflow ID reuse and returns nil without writes on closed old chain", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		c1ID := uuid.NewString()
		_, c1MS, c1WFT := env.addRealOpenRun(c1ID)
		env.setCurrentRun(c1ID)

		req1 := uuid.NewString()
		caller1 := newCallerInfo(env.namespaceID, req1)
		sub1, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller1,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req1,
			Callback:    makeCallerCallback(t, caller1, req1, true),
		})
		require.NoError(t, err)

		// C1 -> C2, then C2 closes (subscription is already settled).
		c2Run, c2MS, c2WFT := env.continueAsNewRun(c1ID, c1MS, c1WFT.GetEventId())
		env.completeRun(c2Run.key.RunID, c2MS, c2WFT.GetEventId(), []byte("c2-done"))

		// Unrelated chain R2 reuses the same workflow ID and gets its own active subscription.
		r2ID := uuid.NewString()
		r2Run, r2MS, _ := env.addRealOpenRun(r2ID)
		env.setCurrentRun(r2ID)

		req2 := uuid.NewString()
		caller2 := newCallerInfo(env.namespaceID, req2)
		_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller2,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req2,
			Callback:    makeCallerCallback(t, caller2, req2, true),
		})
		require.NoError(t, err)
		require.Equal(t, 1, r2Run.updateCalls)

		// Cancel C1's subscription: must not mutate R2; walks C1 -> closed C2 and returns nil with zero writes.
		require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token))

		require.Equal(t, 1, r2Run.updateCalls, "unrelated reuse R2 must not be mutated")
		r2WF, r2Ctx, err := r2MS.ChasmWorkflowComponentReadOnly(t.Context())
		require.NoError(t, err)
		r2CB, ok := r2WF.FindResultSubscription(r2Ctx, req2)
		require.True(t, ok)
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, r2CB.Status)

		require.Zero(t, c2Run.updateCalls, "closed terminal run must not be mutated on cancel")
		require.Empty(t, c2Run.committedEvents)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("cancel on closed run or purged run returns nil with zero writes", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		runID := uuid.NewString()
		run, ms, wftCompleted := env.addRealOpenRun(runID)
		env.setCurrentRun(runID)

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		sub, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    makeCallerCallback(t, caller, reqID, true),
		})
		require.NoError(t, err)
		env.completeRun(runID, ms, wftCompleted.GetEventId(), []byte("done"))

		require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, sub.Subscribed.Token))
		require.Equal(t, 1, run.updateCalls)

		// Purged/absent workflow also returns nil with zero writes.
		purgedTok, err := encodeResultSubscriptionToken(&tokenspb.ResultSubscriptionToken{
			Version:       1,
			NamespaceId:   env.namespaceID.String(),
			WorkflowId:    "purged-wf-" + uuid.NewString(),
			FirstRunId:    uuid.NewString(),
			ResolvedRunId: uuid.NewString(),
			RequestId:     reqID,
		})
		require.NoError(t, err)
		require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, purgedTok))
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("forged token pointing resolved run to unrelated chain is rejected without writes", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		r2ID := uuid.NewString()
		r2Run, _, _ := env.addRealOpenRun(r2ID)
		env.setCurrentRun(r2ID)

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		forgedTok, err := encodeResultSubscriptionToken(&tokenspb.ResultSubscriptionToken{
			Version:       1,
			NamespaceId:   env.namespaceID.String(),
			WorkflowId:    env.workflowID,
			FirstRunId:    uuid.NewString(), // does not match R2's FirstRunID
			ResolvedRunId: r2ID,
			RequestId:     reqID,
		})
		require.NoError(t, err)

		err = Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, forgedTok)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
		require.Zero(t, r2Run.updateCalls)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("unrelated zombie reuse does not block Cancel from walking old chain", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		c1ID := uuid.NewString()
		_, c1MS, c1WFT := env.addRealOpenRun(c1ID)
		env.setCurrentRun(c1ID)

		req1 := uuid.NewString()
		caller1 := newCallerInfo(env.namespaceID, req1)
		sub1, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller1,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   req1,
			Callback:    makeCallerCallback(t, caller1, req1, true),
		})
		require.NoError(t, err)

		c2Run, c2MS, c2WFT := env.continueAsNewRun(c1ID, c1MS, c1WFT.GetEventId())
		env.completeRun(c2Run.key.RunID, c2MS, c2WFT.GetEventId(), []byte("done"))

		// Current run is a zombie execution from an unrelated chain R2.
		r2ID := uuid.NewString()
		r2MS := historyi.NewMockMutableState(env.ctrl)
		r2MS.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
		r2MS.EXPECT().GetFirstRunID(gomock.Any()).Return(r2ID, nil).AnyTimes()
		r2MS.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
			RunId: r2ID,
			State: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		}).AnyTimes()
		r2Run := &apiRun{
			key: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, r2ID),
			ms:  r2MS,
		}
		env.cache.addRun(r2Run)
		env.setCurrentRun(r2ID)

		require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token))
		require.Zero(t, r2Run.updateCalls)
		require.Zero(t, c2Run.updateCalls)
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})

	t.Run("Cancel retries stale current run after CAN and cancels on new run", func(t *testing.T) {
		t.Parallel()
		env := newAPITestEnv(t, true)
		c1ID := uuid.NewString()
		_, c1MS, c1WFT := env.addRealOpenRun(c1ID)
		env.setCurrentRun(c1ID)

		reqID := uuid.NewString()
		caller := newCallerInfo(env.namespaceID, reqID)
		sub, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
			NamespaceID: env.namespaceID,
			Caller:      caller,
			Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
			RequestID:   reqID,
			Callback:    makeCallerCallback(t, caller, reqID, true),
		})
		require.NoError(t, err)

		c2Run, c2MS, _ := env.continueAsNewRun(c1ID, c1MS, c1WFT.GetEventId())
		// First lookup sees stale superseded C1; retry observes C2.
		env.setCurrentSequence(
			currentExecutionResponse{runID: c1ID},
			currentExecutionResponse{runID: c2Run.key.RunID},
		)

		require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, sub.Subscribed.Token))
		require.Equal(t, 1, c2Run.updateCalls)

		c2WF, c2Ctx, err := c2MS.ChasmWorkflowComponentReadOnly(t.Context())
		require.NoError(t, err)
		cb, ok := c2WF.FindResultSubscription(c2Ctx, reqID)
		require.True(t, ok)
		require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
		require.NotNil(t, cb.GetLastAttemptFailure().GetCanceledFailureInfo())
		env.cache.requireSingleRunLockAtATime(t)
		env.cache.requireNoLeakedLocks(t)
	})
}

func requireCallbackCountAndState(
	t *testing.T,
	ms *workflow.MutableStateImpl,
	chasmMode bool,
	wantCount int,
	reqID string,
	wantChasmStatus callbackspb.CallbackStatus,
	wantHSMState enumsspb.CallbackState,
	wantCanceledFailure bool,
) {
	t.Helper()
	if chasmMode {
		wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
		require.NoError(t, err)
		require.Len(t, wf.Callbacks, wantCount)
		require.Equal(t, int64(wantCount), wf.GetCallbackMetadata().GetTotalCallbacksCount())
		if reqID != "" {
			cb, ok := wf.FindResultSubscription(ctx, reqID)
			require.True(t, ok)
			require.Equal(t, wantChasmStatus, cb.Status)
			if wantCanceledFailure {
				require.NotNil(t, cb.GetLastAttemptFailure().GetCanceledFailureInfo())
			}
		}
		return
	}
	coll := hsmcallbacks.MachineCollection(ms.HSM())
	require.Equal(t, wantCount, coll.Size())
	if reqID != "" {
		var found bool
		for _, node := range coll.List() {
			cb, err := coll.Data(node.Key.ID)
			require.NoError(t, err)
			if cb.GetCallback().GetResultSubscription().GetRequestId() == reqID {
				found = true
				require.Equal(t, wantHSMState, cb.State())
				if wantCanceledFailure {
					require.NotNil(t, cb.GetLastAttemptFailure().GetCanceledFailureInfo())
				}
			}
		}
		require.True(t, found, "expected HSM result subscription %s", reqID)
	}
}

func (e *apiTestEnv) transitionViaRetryOrCron(
	prevRunID string,
	prevMS *workflow.MutableStateImpl,
	initiator enumspb.ContinueAsNewInitiator,
	closePrev func(nextRunID string) *historypb.HistoryEvent,
) (*apiRun, *workflow.MutableStateImpl, *historypb.HistoryEvent) {
	e.t.Helper()
	nextRunID := uuid.NewString()
	closeEv := closePrev(nextRunID)
	e.recordEvent(prevRunID, closeEv)
	_, _, err := prevMS.CloseTransactionAsMutation(e.t.Context(), historyi.TransactionPolicyActive)
	require.NoError(e.t, err)

	startEv, err := prevMS.GetStartEvent(e.t.Context())
	require.NoError(e.t, err)

	newMS, err := workflow.NewMutableStateInChain(
		e.testShard,
		e.testShard.MockEventsCache,
		e.testShard.GetLogger(),
		e.nsEntry,
		e.workflowID,
		nextRunID,
		time.Now().UTC(),
		prevMS,
	)
	require.NoError(e.t, err)
	require.NoError(e.t, workflow.SetupNewWorkflowForRetryOrCron(
		e.t.Context(),
		prevMS,
		newMS,
		nextRunID,
		startEv.GetWorkflowExecutionStartedEventAttributes(),
		startEv.GetLinks(),
		nil,
		nil,
		time.Second,
		initiator,
	))

	tq := &taskqueuepb.TaskQueue{Name: "tq"}
	wft, err := newMS.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	require.NoError(e.t, err)
	_, wft, err = newMS.AddWorkflowTaskStartedEvent(wft.ScheduledEventID, "", tq, "", nil, nil, nil, false, nil, 0, "")
	require.NoError(e.t, err)
	completedEv, err := newMS.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		historyi.WorkflowTaskCompletionLimits{MaxResetPoints: 10, MaxSearchAttributeValueSize: 1024},
	)
	require.NoError(e.t, err)
	e.recordEvent(nextRunID, completedEv)
	_, _, err = newMS.CloseTransactionAsSnapshot(e.t.Context(), historyi.TransactionPolicyActive)
	require.NoError(e.t, err)

	run := &apiRun{
		key: definition.NewWorkflowKey(e.namespaceID.String(), e.workflowID, nextRunID),
		ms:  newMS,
	}
	e.cache.addRun(run)
	e.setCurrentRun(nextRunID)
	return run, newMS, completedEv
}

func (e *apiTestEnv) evictAndReloadRun(run *apiRun, ms *workflow.MutableStateImpl) *workflow.MutableStateImpl {
	e.t.Helper()
	dbRecord := ms.CloneToProto()
	reloaded, err := workflow.NewMutableStateFromDB(
		e.testShard,
		e.testShard.MockEventsCache,
		e.testShard.GetLogger(),
		e.nsEntry,
		dbRecord,
		1,
	)
	require.NoError(e.t, err)
	require.NoError(e.t, reloaded.UpdateCurrentVersion(e.nsEntry.FailoverVersion(e.workflowID), true))
	run.mu.Lock()
	run.ms = reloaded
	run.mu.Unlock()
	return reloaded
}

func TestCancelCloseConcurrencyBothLockOrders(t *testing.T) {
	t.Parallel()

	for _, chasmMode := range []bool{true, false} {
		modeName := "HSM"
		if chasmMode {
			modeName = "CHASM"
		}

		t.Run(modeName+"_cancel_wins_before_close", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			runID := uuid.NewString()
			run, ms, wftCompleted := env.addRealOpenRun(runID)
			env.setCurrentRun(runID)

			req1, req2 := uuid.NewString(), uuid.NewString()
			caller1, caller2 := newCallerInfo(env.namespaceID, req1), newCallerInfo(env.namespaceID, req2)
			sub1, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller1,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req1,
				Callback:    makeCallerCallback(t, caller1, req1, chasmMode),
			})
			require.NoError(t, err)
			_, err = Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller2,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req2,
				Callback:    makeCallerCallback(t, caller2, req2, chasmMode),
			})
			require.NoError(t, err)

			cancelInUpdate := make(chan struct{})
			releaseCancel := make(chan struct{})
			run.updateFn = func(ctx context.Context, m historyi.MutableState) error {
				close(cancelInUpdate)
				<-releaseCancel
				_, batches, err := m.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
				if err != nil {
					return err
				}
				run.mu.Lock()
				for _, b := range batches {
					run.committedEvents = append(run.committedEvents, b.Events...)
				}
				run.mu.Unlock()
				return nil
			}

			cancelDone := make(chan error, 1)
			go func() {
				cancelDone <- Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token)
			}()

			<-cancelInUpdate

			closeDone := make(chan error, 1)
			go func() {
				_, releaseFn, err := env.cache.GetOrCreateWorkflowExecution(
					t.Context(),
					env.testShard,
					env.namespaceID,
					&commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: runID},
					locks.PriorityHigh,
				)
				if err != nil {
					closeDone <- err
					return
				}
				env.completeRun(runID, ms, wftCompleted.GetEventId(), []byte("closed-after-cancel"))
				releaseFn(nil)
				closeDone <- nil
			}()

			close(releaseCancel)
			require.NoError(t, <-cancelDone)
			require.NoError(t, <-closeDone)

			// Waiter 1 stayed FAILED (canceled) with no delivery scheduled; waiter 2 was scheduled.
			requireCallbackCountAndState(t, ms, chasmMode, 2, req1, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			requireCallbackCountAndState(t, ms, chasmMode, 2, req2, callbackspb.CALLBACK_STATUS_SCHEDULED, enumsspb.CALLBACK_STATE_SCHEDULED, false)
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})

		t.Run(modeName+"_close_wins_before_cancel", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			runID := uuid.NewString()
			run, ms, wftCompleted := env.addRealOpenRun(runID)
			env.setCurrentRun(runID)

			reqID := uuid.NewString()
			caller := newCallerInfo(env.namespaceID, reqID)
			sub, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqID,
				Callback:    makeCallerCallback(t, caller, reqID, chasmMode),
			})
			require.NoError(t, err)
			require.Equal(t, 1, run.updateCalls)

			// Close holds the run lease first while Cancel waits for the same lease.
			_, releaseClose, err := env.cache.GetOrCreateWorkflowExecution(
				t.Context(),
				env.testShard,
				env.namespaceID,
				&commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: runID},
				locks.PriorityHigh,
			)
			require.NoError(t, err)

			cancelDone := make(chan error, 1)
			go func() {
				cancelDone <- Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, sub.Subscribed.Token)
			}()

			env.completeRun(runID, ms, wftCompleted.GetEventId(), []byte("closed-first"))
			releaseClose(nil)

			require.NoError(t, <-cancelDone)
			require.Equal(t, 1, run.updateCalls, "Cancel after Close must be a no-op with zero writes")
			requireCallbackCountAndState(t, ms, chasmMode, 1, reqID, callbackspb.CALLBACK_STATUS_SCHEDULED, enumsspb.CALLBACK_STATE_SCHEDULED, false)
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})

		t.Run(modeName+"_stale_cache_conflict_when_close_commits_concurrently", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			runID := uuid.NewString()
			staleRun, staleMS, wftCompleted := env.addRealOpenRun(runID)
			env.setCurrentRun(runID)

			reqID := uuid.NewString()
			caller := newCallerInfo(env.namespaceID, reqID)
			sub, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqID,
				Callback:    makeCallerCallback(t, caller, reqID, chasmMode),
			})
			require.NoError(t, err)

			// Snapshot before close and build a closed DB copy representing the concurrently committed close.
			closedMS, err := workflow.NewMutableStateFromDB(
				env.testShard, env.testShard.MockEventsCache, env.testShard.GetLogger(), env.nsEntry, staleMS.CloneToProto(), 1,
			)
			require.NoError(t, err)
			require.NoError(t, closedMS.UpdateCurrentVersion(env.nsEntry.FailoverVersion(env.workflowID), true))
			env.completeRun(runID, closedMS, wftCompleted.GetEventId(), []byte("concurrent-close"))

			var cleared atomic.Bool
			staleRun.loadFn = func(context.Context) (historyi.MutableState, error) {
				if cleared.Load() {
					return closedMS, nil
				}
				return staleMS, nil
			}
			staleRun.clearFn = func() { cleared.Store(true) }
			staleRun.updateFn = func(context.Context, historyi.MutableState) error {
				return &persistence.CurrentWorkflowConditionFailedError{Msg: "run closed concurrently", RunID: runID}
			}

			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, sub.Subscribed.Token))
			require.True(t, cleared.Load())
			requireCallbackCountAndState(t, closedMS, chasmMode, 1, reqID, callbackspb.CALLBACK_STATUS_SCHEDULED, enumsspb.CALLBACK_STATE_SCHEDULED, false)
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})

		t.Run(modeName+"_concurrent_repeated_cancels_write_once", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			runID := uuid.NewString()
			run, ms, _ := env.addRealOpenRun(runID)
			env.setCurrentRun(runID)

			reqID := uuid.NewString()
			caller := newCallerInfo(env.namespaceID, reqID)
			sub, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqID,
				Callback:    makeCallerCallback(t, caller, reqID, chasmMode),
			})
			require.NoError(t, err)
			require.Equal(t, 1, run.updateCalls)

			const workers = 16
			errs := make(chan error, workers)
			gate := make(chan struct{})
			var wg sync.WaitGroup
			for range workers {
				wg.Go(func() {
					<-gate
					errs <- Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller, sub.Subscribed.Token)
				})
			}
			close(gate)
			wg.Wait()
			close(errs)

			for err := range errs {
				require.NoError(t, err)
			}
			require.Equal(t, 2, run.updateCalls, "concurrent repeated Cancels must commit exactly one cancel event")
			requireCallbackCountAndState(t, ms, chasmMode, 1, reqID, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})

		t.Run(modeName+"_already_delivered_cancel_noop_and_DEC2_late_attach", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			run1ID := uuid.NewString()
			run1, ms1, wft1 := env.addRealOpenRun(run1ID)
			env.setCurrentRun(run1ID)

			reqDelivered := uuid.NewString()
			callerDelivered := newCallerInfo(env.namespaceID, reqDelivered)
			subDelivered, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      callerDelivered,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqDelivered,
				Callback:    makeCallerCallback(t, callerDelivered, reqDelivered, chasmMode),
			})
			require.NoError(t, err)

			// Close run1 and mark callback SUCCEEDED (already delivered).
			env.completeRun(run1ID, ms1, wft1.GetEventId(), []byte("done"))
			if chasmMode {
				wf, writeCtx, err := ms1.ChasmWorkflowComponent(t.Context())
				require.NoError(t, err)
				cb, ok := wf.FindResultSubscription(writeCtx, reqDelivered)
				require.True(t, ok)
				require.NoError(t, chasmcallback.TransitionSucceeded.Apply(cb, writeCtx, chasmcallback.EventSucceeded{Time: time.Now().UTC()}))
			} else {
				coll := hsmcallbacks.MachineCollection(ms1.HSM())
				//nolint:staticcheck // SA1019: HSM callbacks remain supported alongside CHASM.
				require.NoError(t, coll.Transition("cb-"+reqDelivered+"-0", func(cb hsmcallbacks.Callback) (hsm.TransitionOutput, error) {
					return hsmcallbacks.TransitionSucceeded.Apply(cb, hsmcallbacks.EventSucceeded{Time: time.Now().UTC()})
				}))
			}
			updatesBefore := run1.updateCalls
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, callerDelivered, subDelivered.Subscribed.Token))
			require.Equal(t, updatesBefore, run1.updateCalls)
			requireCallbackCountAndState(t, ms1, chasmMode, 1, reqDelivered, callbackspb.CALLBACK_STATUS_SUCCEEDED, enumsspb.CALLBACK_STATE_SUCCEEDED, false)

			// DEC-2: on a new open chain, explicit cancel followed by same-run Start dedup does not resurrect,
			// and after CAN drops the canceled callback (DEC-8, no tombstones) a late Start is allowed once.
			run2ID := uuid.NewString()
			run2, ms2, wft2 := env.addRealOpenRun(run2ID)
			env.setCurrentRun(run2ID)
			reqLate := uuid.NewString()
			callerLate := newCallerInfo(env.namespaceID, reqLate)
			cbLate := makeCallerCallback(t, callerLate, reqLate, chasmMode)
			subLate, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      callerLate,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqLate,
				Callback:    cbLate,
			})
			require.NoError(t, err)
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, callerLate, subLate.Subscribed.Token))

			// Same-run Start retry returns original token without resurrecting the canceled callback.
			dupSameRun, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      callerLate,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqLate,
				Callback:    cbLate,
			})
			require.NoError(t, err)
			require.Equal(t, subLate.Subscribed.Token, dupSameRun.Subscribed.Token)
			require.Equal(t, 2, run2.updateCalls)
			requireCallbackCountAndState(t, ms2, chasmMode, 1, reqLate, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)

			// CAN drops canceled callback (DEC-8, no tombstones); a late Start on the new run is allowed once (DEC-2).
			run3, ms3, _ := env.continueAsNewRun(run2ID, ms2, wft2.GetEventId())
			requireCallbackCountAndState(t, ms3, chasmMode, 0, "", 0, 0, false)
			lateAttach, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      callerLate,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   reqLate,
				Callback:    cbLate,
			})
			require.NoError(t, err)
			require.NotNil(t, lateAttach.Subscribed)
			require.Equal(t, 1, run3.updateCalls)
			requireCallbackCountAndState(t, ms3, chasmMode, 1, reqLate, callbackspb.CALLBACK_STATUS_STANDBY, enumsspb.CALLBACK_STATE_STANDBY, false)
			env.cache.requireNoLeakedLocks(t)
		})
	}
}

func TestLifecycleAcrossCronRetryCacheEvictionResetAndFailover(t *testing.T) {
	t.Parallel()

	for _, chasmMode := range []bool{true, false} {
		modeName := "HSM"
		if chasmMode {
			modeName = "CHASM"
		}

		t.Run(modeName+"_cron_and_retry_carry_over_stable_token_and_DEC8", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			legacyCB := &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: "http://legacy.example/cb"},
				},
			}
			run1ID := uuid.NewString()
			_, ms1, wft1 := env.addRealOpenRun(run1ID, legacyCB)
			env.setCurrentRun(run1ID)

			req1, req2 := uuid.NewString(), uuid.NewString()
			caller1, caller2 := newCallerInfo(env.namespaceID, req1), newCallerInfo(env.namespaceID, req2)
			cb1 := makeCallerCallback(t, caller1, req1, chasmMode)
			cb2 := makeCallerCallback(t, caller2, req2, chasmMode)

			sub1, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller1,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req1,
				Callback:    cb1,
			})
			require.NoError(t, err)
			sub2, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller2,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req2,
				Callback:    cb2,
			})
			require.NoError(t, err)
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller2, sub2.Subscribed.Token))

			// 1. Cron completion: run1 -> run2. Legacy fires on run1 & is not copied; sub2 (canceled) is not copied (DEC-8);
			//    sub1 stays STANDBY on run1 & is carried to run2.
			_, ms2, wft2 := env.transitionViaRetryOrCron(run1ID, ms1, enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, func(nextRunID string) *historypb.HistoryEvent {
				ev, err := ms1.AddCompletedWorkflowEvent(wft1.GetEventId(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, nextRunID)
				require.NoError(t, err)
				return ev
			})
			requireCallbackCountAndState(t, ms1, chasmMode, 3, req1, callbackspb.CALLBACK_STATUS_STANDBY, enumsspb.CALLBACK_STATE_STANDBY, false)
			requireCallbackCountAndState(t, ms1, chasmMode, 3, req2, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			requireCallbackCountAndState(t, ms2, chasmMode, 1, req1, callbackspb.CALLBACK_STATUS_STANDBY, enumsspb.CALLBACK_STATE_STANDBY, false)

			// 2. Retry on failure (RETRY_STATE_IN_PROGRESS): run2 -> run3.
			_, ms3, wft3 := env.transitionViaRetryOrCron(ms2.GetExecutionState().GetRunId(), ms2, enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY, func(nextRunID string) *historypb.HistoryEvent {
				ev, err := ms2.AddFailWorkflowEvent(
					wft2.GetEventId(),
					enumspb.RETRY_STATE_IN_PROGRESS,
					&commandpb.FailWorkflowExecutionCommandAttributes{Failure: &failurepb.Failure{Message: "retryable"}},
					nextRunID,
				)
				require.NoError(t, err)
				return ev
			})
			requireCallbackCountAndState(t, ms3, chasmMode, 1, req1, callbackspb.CALLBACK_STATUS_STANDBY, enumsspb.CALLBACK_STATE_STANDBY, false)

			// 3. Cron failure (no retry): run3 -> run4.
			_, ms4, _ := env.transitionViaRetryOrCron(ms3.GetExecutionState().GetRunId(), ms3, enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, func(nextRunID string) *historypb.HistoryEvent {
				ev, err := ms3.AddFailWorkflowEvent(
					wft3.GetEventId(),
					enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
					&commandpb.FailWorkflowExecutionCommandAttributes{Failure: &failurepb.Failure{Message: "cron fail"}},
					nextRunID,
				)
				require.NoError(t, err)
				return ev
			})
			requireCallbackCountAndState(t, ms4, chasmMode, 1, req1, callbackspb.CALLBACK_STATUS_STANDBY, enumsspb.CALLBACK_STATE_STANDBY, false)

			// 4. Cron timeout (no retry): run4 -> run5.
			run5, ms5, _ := env.transitionViaRetryOrCron(ms4.GetExecutionState().GetRunId(), ms4, enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, func(nextRunID string) *historypb.HistoryEvent {
				ev, err := ms4.AddTimeoutWorkflowEvent(enumspb.RETRY_STATE_TIMEOUT, nextRunID)
				require.NoError(t, err)
				return ev
			})
			requireCallbackCountAndState(t, ms5, chasmMode, 1, req1, callbackspb.CALLBACK_STATUS_STANDBY, enumsspb.CALLBACK_STATE_STANDBY, false)

			// Verify original metadata preserved on run5.
			stored, found, err := findResultSubscription(t.Context(), ms5, req1)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int32(1), stored.sub.GetResultFormatVersion())
			protorequire.ProtoEqual(t, &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: run1ID}, stored.sub.GetResolvedExecution())

			// Start dedup (unpinned and pinned to run1) returns the exact original token with zero writes.
			for _, targetRunID := range []string{"", run1ID} {
				dup, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
					NamespaceID: env.namespaceID,
					Caller:      caller1,
					Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: targetRunID},
					RequestID:   req1,
					Callback:    cb1,
				})
				require.NoError(t, err)
				require.Equal(t, sub1.Subscribed.Token, dup.Subscribed.Token)
				protorequire.ProtoEqual(t, sub1.Subscribed.Link, dup.Subscribed.Link)
				require.Zero(t, run5.updateCalls)
			}

			// Repeated Cancel of dropped sub2 is a no-op with zero writes.
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller2, sub2.Subscribed.Token))
			require.Zero(t, run5.updateCalls)

			// Cancel of sub1 with the original run1 token finds and cancels it on run5.
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token))
			require.Equal(t, 1, run5.updateCalls)
			requireCallbackCountAndState(t, ms5, chasmMode, 1, req1, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})

		t.Run(modeName+"_cache_eviction_reload_preserves_token_and_cancel", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			runID := uuid.NewString()
			run, ms, _ := env.addRealOpenRun(runID)
			env.setCurrentRun(runID)

			req1, req2 := uuid.NewString(), uuid.NewString()
			caller1, caller2 := newCallerInfo(env.namespaceID, req1), newCallerInfo(env.namespaceID, req2)
			cb1 := makeCallerCallback(t, caller1, req1, chasmMode)
			cb2 := makeCallerCallback(t, caller2, req2, chasmMode)

			sub1, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller1,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req1,
				Callback:    cb1,
			})
			require.NoError(t, err)
			sub2, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller2,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req2,
				Callback:    cb2,
			})
			require.NoError(t, err)
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller2, sub2.Subscribed.Token))
			require.Equal(t, 3, run.updateCalls)

			// Simulate mutable-state cache eviction and reload from persistence.
			reloaded := env.evictAndReloadRun(run, ms)

			// Dedup and repeated cancel work against the reloaded mutable state with zero writes.
			dup, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller1,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req1,
				Callback:    cb1,
			})
			require.NoError(t, err)
			require.Equal(t, sub1.Subscribed.Token, dup.Subscribed.Token)
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller2, sub2.Subscribed.Token))
			require.Equal(t, 3, run.updateCalls)

			// Cancel sub1 after eviction, then evict and reload again to confirm persisted disposition.
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token))
			require.Equal(t, 4, run.updateCalls)
			reloaded2 := env.evictAndReloadRun(run, reloaded)

			requireCallbackCountAndState(t, reloaded2, chasmMode, 2, req1, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			requireCallbackCountAndState(t, reloaded2, chasmMode, 2, req2, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token))
			require.Equal(t, 4, run.updateCalls)
			env.cache.requireNoLeakedLocks(t)
		})

		t.Run(modeName+"_reset_reapply_preserves_token_and_blocks_resurrection", func(t *testing.T) {
			t.Parallel()
			env := newAPITestEnv(t, chasmMode)
			baseRunID := uuid.NewString()
			baseRun, baseMS, _ := env.addRealOpenRun(baseRunID)
			env.setCurrentRun(baseRunID)

			req1, req2 := uuid.NewString(), uuid.NewString()
			caller1, caller2 := newCallerInfo(env.namespaceID, req1), newCallerInfo(env.namespaceID, req2)
			cb1 := makeCallerCallback(t, caller1, req1, chasmMode)
			cb2 := makeCallerCallback(t, caller2, req2, chasmMode)

			sub1, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller1,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req1,
				Callback:    cb1,
			})
			require.NoError(t, err)
			sub2, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
				NamespaceID: env.namespaceID,
				Caller:      caller2,
				Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID},
				RequestID:   req2,
				Callback:    cb2,
			})
			require.NoError(t, err)
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller2, sub2.Subscribed.Token))

			// Rebuild reset run from baseRun's start event and reapply post-reset attach/cancel events
			// (plus a duplicate old attach for canceled sub2 to confirm no resurrection).
			resetRunID := uuid.NewString()
			startEv, err := baseMS.GetStartEvent(t.Context())
			require.NoError(t, err)
			resetMS := workflow.NewMutableState(
				env.testShard, env.testShard.MockEventsCache, env.testShard.GetLogger(), env.nsEntry, env.workflowID, resetRunID, time.Now().UTC(),
			)
			rebuilder := workflow.NewMutableStateRebuilder(env.testShard, env.testShard.GetLogger(), resetMS)
			_, err = rebuilder.ApplyEvents(
				t.Context(),
				env.namespaceID,
				"reset-req",
				&commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: resetRunID},
				[][]*historypb.HistoryEvent{{startEv}},
				nil,
				"",
			)
			require.NoError(t, err)
			require.NoError(t, resetMS.UpdateCurrentVersion(env.nsEntry.FailoverVersion(env.workflowID), true))
			for _, ev := range baseRun.committedEvents {
				_, err = resetMS.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
					ev.GetWorkflowExecutionOptionsUpdatedEventAttributes(), ev.GetLinks(),
				)
				require.NoError(t, err)
			}
			// Replaying the old attach event for canceled sub2 must not resurrect it.
			require.NoError(t, resetMS.ApplyWorkflowExecutionOptionsUpdatedEvent(baseRun.committedEvents[1]))
			_, _, err = resetMS.CloseTransactionAsSnapshot(t.Context(), historyi.TransactionPolicyActive)
			require.NoError(t, err)

			// Mark base run as reset -> resetRunID and register resetRun as current.
			baseMS.UpdateResetRunID(resetRunID)
			baseMS.GetExecutionInfo().WorkflowWasReset = true
			require.NoError(t, workflow.TerminateWorkflow(baseMS, "reset", nil, "history-service", false, nil))
			_, _, err = baseMS.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyActive)
			require.NoError(t, err)

			resetRun := &apiRun{
				key: definition.NewWorkflowKey(env.namespaceID.String(), env.workflowID, resetRunID),
				ms:  resetMS,
			}
			env.cache.addRun(resetRun)
			env.setCurrentRun(resetRunID)

			// Verify 2 callbacks (sub1 STANDBY, sub2 FAILED-canceled), original token stable via pinned & unpinned Start.
			requireCallbackCountAndState(t, resetMS, chasmMode, 2, req1, callbackspb.CALLBACK_STATUS_STANDBY, enumsspb.CALLBACK_STATE_STANDBY, false)
			requireCallbackCountAndState(t, resetMS, chasmMode, 2, req2, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			for _, targetRunID := range []string{"", baseRunID} {
				dup, err := Start(t.Context(), env.testShard, env.checker, StartRequest{
					NamespaceID: env.namespaceID,
					Caller:      caller1,
					Target:      &commonpb.WorkflowExecution{WorkflowId: env.workflowID, RunId: targetRunID},
					RequestID:   req1,
					Callback:    cb1,
				})
				require.NoError(t, err)
				require.Equal(t, sub1.Subscribed.Token, dup.Subscribed.Token)
				require.Zero(t, resetRun.updateCalls)
			}

			// Cancel sub1 with original token succeeds on reset run; Cancel sub2 is a no-op.
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller2, sub2.Subscribed.Token))
			require.Zero(t, resetRun.updateCalls)
			require.NoError(t, Cancel(t.Context(), env.testShard, env.checker, env.namespaceID, caller1, sub1.Subscribed.Token))
			require.Equal(t, 1, resetRun.updateCalls)
			requireCallbackCountAndState(t, resetMS, chasmMode, 2, req1, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			env.cache.requireSingleRunLockAtATime(t)
			env.cache.requireNoLeakedLocks(t)
		})

		t.Run(modeName+"_replication_and_failover_preserves_token_and_cancel", func(t *testing.T) {
			t.Parallel()
			activeEnv := newAPITestEnv(t, chasmMode)
			run1ID := uuid.NewString()
			run1, ms1, wft1 := activeEnv.addRealOpenRun(run1ID)
			activeEnv.setCurrentRun(run1ID)

			req1, req2 := uuid.NewString(), uuid.NewString()
			caller1, caller2 := newCallerInfo(activeEnv.namespaceID, req1), newCallerInfo(activeEnv.namespaceID, req2)
			cb1 := makeCallerCallback(t, caller1, req1, chasmMode)
			cb2 := makeCallerCallback(t, caller2, req2, chasmMode)

			sub1, err := Start(t.Context(), activeEnv.testShard, activeEnv.checker, StartRequest{
				NamespaceID: activeEnv.namespaceID,
				Caller:      caller1,
				Target:      &commonpb.WorkflowExecution{WorkflowId: activeEnv.workflowID},
				RequestID:   req1,
				Callback:    cb1,
			})
			require.NoError(t, err)
			sub2, err := Start(t.Context(), activeEnv.testShard, activeEnv.checker, StartRequest{
				NamespaceID: activeEnv.namespaceID,
				Caller:      caller2,
				Target:      &commonpb.WorkflowExecution{WorkflowId: activeEnv.workflowID},
				RequestID:   req2,
				Callback:    cb2,
			})
			require.NoError(t, err)
			require.NoError(t, Cancel(t.Context(), activeEnv.testShard, activeEnv.checker, activeEnv.namespaceID, caller2, sub2.Subscribed.Token))

			// Cron close run1 -> run2 and capture full histories of both runs for replication.
			run2ID := uuid.NewString()
			start1Ev, err := ms1.GetStartEvent(t.Context())
			require.NoError(t, err)
			close1Ev, err := ms1.AddCompletedWorkflowEvent(wft1.GetEventId(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, run2ID)
			require.NoError(t, err)
			_, _, err = ms1.CloseTransactionAsMutation(t.Context(), historyi.TransactionPolicyActive)
			require.NoError(t, err)

			ms2Active, err := workflow.NewMutableStateInChain(
				activeEnv.testShard, activeEnv.testShard.MockEventsCache, activeEnv.testShard.GetLogger(),
				activeEnv.nsEntry, activeEnv.workflowID, run2ID, time.Now().UTC(), ms1,
			)
			require.NoError(t, err)
			require.NoError(t, workflow.SetupNewWorkflowForRetryOrCron(
				t.Context(), ms1, ms2Active, run2ID,
				start1Ev.GetWorkflowExecutionStartedEventAttributes(), start1Ev.GetLinks(), nil, nil,
				time.Second, enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
			))
			_, run2Batches, err := ms2Active.CloseTransactionAsSnapshot(t.Context(), historyi.TransactionPolicyActive)
			require.NoError(t, err)

			var run1ReplEvents []*historypb.HistoryEvent
			run1ReplEvents = append(run1ReplEvents, run1.initEvents...)
			run1ReplEvents = append(run1ReplEvents, run1.committedEvents...)
			run1ReplEvents = append(run1ReplEvents, close1Ev)
			var run2ReplEvents []*historypb.HistoryEvent
			for _, b := range run2Batches {
				run2ReplEvents = append(run2ReplEvents, b.Events...)
			}

			// Replicate onto a separate cluster environment (standby -> promoted active on failover).
			failoverEnv := newAPITestEnv(t, chasmMode)
			failoverEnv.workflowID = activeEnv.workflowID
			standbyRun1 := workflow.NewMutableState(
				failoverEnv.testShard, failoverEnv.testShard.MockEventsCache, failoverEnv.testShard.GetLogger(),
				failoverEnv.nsEntry, failoverEnv.workflowID, run1ID, time.Now().UTC(),
			)
			rebuilder := workflow.NewMutableStateRebuilder(failoverEnv.testShard, failoverEnv.testShard.GetLogger(), standbyRun1)
			var run1EventBatches [][]*historypb.HistoryEvent
			for _, ev := range run1ReplEvents {
				run1EventBatches = append(run1EventBatches, []*historypb.HistoryEvent{ev})
			}
			replicatedRun2Iface, err := rebuilder.ApplyEvents(
				t.Context(),
				failoverEnv.namespaceID,
				"repl-start-req",
				&commonpb.WorkflowExecution{WorkflowId: failoverEnv.workflowID, RunId: run1ID},
				run1EventBatches,
				run2ReplEvents,
				run2ID,
			)
			require.NoError(t, err)
			replicatedRun2, ok := replicatedRun2Iface.(*workflow.MutableStateImpl)
			require.True(t, ok)
			snap2, _, err := replicatedRun2.CloseTransactionAsSnapshot(t.Context(), historyi.TransactionPolicyPassive)
			require.NoError(t, err)
			promotedRun2, err := workflow.NewMutableStateFromDB(
				failoverEnv.testShard, failoverEnv.testShard.MockEventsCache, failoverEnv.testShard.GetLogger(),
				failoverEnv.nsEntry,
				&persistencespb.WorkflowMutableState{
					ExecutionInfo:  common.CloneProto(snap2.ExecutionInfo),
					ExecutionState: common.CloneProto(snap2.ExecutionState),
					NextEventId:    snap2.NextEventID,
					ChasmNodes:     snap2.ChasmNodes,
					Checksum:       snap2.Checksum,
				},
				1,
			)
			require.NoError(t, err)
			require.NoError(t, promotedRun2.UpdateCurrentVersion(failoverEnv.nsEntry.FailoverVersion(failoverEnv.workflowID), true))

			// Register replicated runs in failover cluster cache (simulating failover promotion).
			failoverEnv.cache.addRun(&apiRun{
				key: definition.NewWorkflowKey(failoverEnv.namespaceID.String(), failoverEnv.workflowID, run1ID),
				ms:  standbyRun1,
			})
			failoverRun2 := &apiRun{
				key: definition.NewWorkflowKey(failoverEnv.namespaceID.String(), failoverEnv.workflowID, run2ID),
				ms:  promotedRun2,
			}
			failoverEnv.cache.addRun(failoverRun2)
			failoverEnv.setCurrentRun(run2ID)

			// Post-failover: run2 has exactly 1 callback (sub1), original metadata, and stable token.
			requireCallbackCountAndState(t, promotedRun2, chasmMode, 1, req1, callbackspb.CALLBACK_STATUS_STANDBY, enumsspb.CALLBACK_STATE_STANDBY, false)
			dupAfterFailover, err := Start(t.Context(), failoverEnv.testShard, failoverEnv.checker, StartRequest{
				NamespaceID: failoverEnv.namespaceID,
				Caller:      caller1,
				Target:      &commonpb.WorkflowExecution{WorkflowId: failoverEnv.workflowID, RunId: run1ID},
				RequestID:   req1,
				Callback:    cb1,
			})
			require.NoError(t, err)
			require.Equal(t, sub1.Subscribed.Token, dupAfterFailover.Subscribed.Token)
			require.Zero(t, failoverRun2.updateCalls)

			// Post-failover Cancel with original token cancels sub1 on run2; replaying old attach cannot resurrect.
			require.NoError(t, Cancel(t.Context(), failoverEnv.testShard, failoverEnv.checker, failoverEnv.namespaceID, caller1, sub1.Subscribed.Token))
			require.Equal(t, 1, failoverRun2.updateCalls)
			require.NoError(t, promotedRun2.ApplyWorkflowExecutionOptionsUpdatedEvent(run1.committedEvents[0]))
			requireCallbackCountAndState(t, promotedRun2, chasmMode, 1, req1, callbackspb.CALLBACK_STATUS_FAILED, enumsspb.CALLBACK_STATE_FAILED, true)
			failoverEnv.cache.requireSingleRunLockAtATime(t)
			failoverEnv.cache.requireNoLeakedLocks(t)
		})
	}
}
