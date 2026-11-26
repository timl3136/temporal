package workflow

import (
	"context"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/callbacks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testCompletionCallbacks(n int) []*commonpb.Callback {
	cbs := make([]*commonpb.Callback, n)
	for i := range cbs {
		cbs[i] = &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/callback"},
			},
		}
	}
	return cbs
}

// enableChasmCallbacks turns on the CHASM callback path and caps the execution at
// maxCallbacks. It returns a fresh mutable state built against the reconfigured shard.
func (s *mutableStateSuite) enableChasmCallbacks(maxCallbacks int) *MutableStateImpl {
	s.mockConfig.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockConfig.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	s.Require().NoError(chasmRegistry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
	s.Require().NoError(chasmRegistry.Register(chasmcallback.NewNilLibrary()))
	s.mockShard.SetChasmRegistry(chasmRegistry)

	hsmReg := hsm.NewRegistry()
	s.Require().NoError(RegisterStateMachine(hsmReg))
	s.Require().NoError(callbacks.RegisterStateMachine(hsmReg))
	s.Require().NoError(callbacks.RegisterTaskSerializers(hsmReg))
	s.mockShard.SetStateMachineRegistry(hsmReg)

	cfg := test.NewCallbacksValidatorConfig()
	cfg.MaxCallbacksPerExecution = func(string) int { return maxCallbacks }
	s.mockShard.SetCallbackValidator(test.NewCallbacksValidator(s.T(), cfg))

	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	return NewMutableState(
		s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)
}

func (s *mutableStateSuite) chasmWorkflowComponent(ms *MutableStateImpl) *chasmworkflow.Workflow {
	wf, _, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
	s.Require().NoError(err)
	return wf
}

func (s *mutableStateSuite) TestChasmCompletionCallbacks_ValidateCallbackAddition() {
	ms := s.enableChasmCallbacks(2)

	s.NoError(ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(2),
	}))

	err := ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(3),
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
	s.ErrorContains(err, "cannot attach more than 2 callbacks to an execution")
}

// Validation is left to request handlers, so attaching never checks the limits, and the totals
// track exactly what was attached.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_AttachingDoesNotValidate() {
	ms := s.enableChasmCallbacks(2)

	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "req-1",
				CompletionCallbacks: testCompletionCallbacks(3),
			},
		},
	)
	s.NoError(err)

	wf := s.chasmWorkflowComponent(ms)
	s.Len(wf.Callbacks, 3)
	s.Equal(int64(3), wf.GetCallbackMetadata().GetTotalCallbacksCount())
	s.NotZero(wf.GetCallbackMetadata().GetTotalCallbacksSize())
}

// The rebuild path replays events another cluster already committed. Re-checking the limit
// there would stall the replication task instead of protecting anything, so an over-limit
// event must still apply cleanly.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_LimitNotEnforcedOnRebuildPath() {
	ms := s.enableChasmCallbacks(2)

	cbs := testCompletionCallbacks(3)
	startEvent := &historypb.HistoryEvent{
		EventId:   common.FirstEventID,
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType:        &commonpb.WorkflowType{Name: "test-workflow-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				CompletionCallbacks: cbs,
			},
		},
	}

	err := ms.ApplyWorkflowExecutionStartedEvent(
		nil,
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		"req-1",
		startEvent,
	)
	s.NoError(err)

	wf := s.chasmWorkflowComponent(ms)
	s.Len(wf.Callbacks, 3)
	s.Equal(int64(3), wf.GetCallbackMetadata().GetTotalCallbacksCount())
}

// lowerCallbackLimits drops the execution-wide limits below anything the tests attach,
// simulating callbacks that were accepted before an operator lowered the limits.
func (s *mutableStateSuite) lowerCallbackLimits() {
	cfg := test.NewCallbacksValidatorConfig()
	cfg.MaxCallbacksPerExecution = func(string) int { return 1 }
	cfg.TotalCallbacksMaxSize = func(string) int { return 1 }
	s.mockShard.SetCallbackValidator(test.NewCallbacksValidator(s.T(), cfg))
}

// startWithCompletionCallbacks starts ms with n completion callbacks and completes its first
// workflow task, leaving it ready to continue-as-new or retry.
func (s *mutableStateSuite) startWithCompletionCallbacks(
	ms *MutableStateImpl,
	n int,
) (startEvent *historypb.HistoryEvent, completedEvent *historypb.HistoryEvent) {
	tq := &taskqueuepb.TaskQueue{Name: "test-task-queue"}
	startEvent, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "req-1",
				WorkflowType:        &commonpb.WorkflowType{Name: "test-workflow-type"},
				TaskQueue:           tq,
				CompletionCallbacks: testCompletionCallbacks(n),
			},
		},
	)
	s.NoError(err)
	s.mockEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(startEvent, nil).AnyTimes()

	wft, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, wft, err = ms.AddWorkflowTaskStartedEvent(wft.ScheduledEventID, "", tq, "", nil, nil, nil, false, nil, 0, "")
	s.NoError(err)
	completedEvent, err = ms.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	s.NoError(err)
	return startEvent, completedEvent
}

// requireCarriedOverCallbacks checks that the new run kept all of the previous run's callbacks,
// and that the lowered limits still reject anything newly attached to it.
func (s *mutableStateSuite) requireCarriedOverCallbacks(newRun historyi.MutableState, want int) {
	wf, _, err := newRun.ChasmWorkflowComponentReadOnly(context.Background())
	s.NoError(err)
	s.Len(wf.Callbacks, want)
	s.Equal(int64(want), wf.GetCallbackMetadata().GetTotalCallbacksCount())

	err = newRun.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-new",
		Callbacks: testCompletionCallbacks(1),
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
}

// Callbacks carried over to a new run were validated when first attached. Re-validating them
// would fail the workflow task completing the continue-as-new, wedging the workflow, so they
// are kept as-is even though the execution is now above the limits.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_ContinueAsNewKeepsCallbacksOverLimit() {
	ms := s.enableChasmCallbacks(10)
	_, completedEvent := s.startWithCompletionCallbacks(ms, 3)
	s.lowerCallbackLimits()

	_, newRun, err := ms.AddContinueAsNewEvent(
		context.Background(),
		completedEvent.GetEventId(),
		"",
		&commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
			WorkflowRunTimeout: ms.GetExecutionInfo().WorkflowRunTimeout,
		},
		nil,
	)
	s.NoError(err)
	s.requireCarriedOverCallbacks(newRun, 3)
}

func (s *mutableStateSuite) TestChasmCompletionCallbacks_RetryKeepsCallbacksOverLimit() {
	ms := s.enableChasmCallbacks(10)
	startEvent, _ := s.startWithCompletionCallbacks(ms, 3)
	s.lowerCallbackLimits()

	newRunID := uuid.NewString()
	newRun, err := NewMutableStateInChain(
		s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, newRunID, time.Now().UTC(), ms,
	)
	s.NoError(err)
	s.NoError(SetupNewWorkflowForRetryOrCron(
		context.Background(),
		ms,
		newRun,
		newRunID,
		startEvent.GetWorkflowExecutionStartedEventAttributes(),
		nil,
		nil,
		&failurepb.Failure{Message: "retryable failure"},
		time.Second,
		enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY,
	))
	s.requireCarriedOverCallbacks(newRun, 3)
}

// Validation must only read the CHASM tree: the update API calls it before deciding whether
// anything will be written at all.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_ValidationIsReadOnly() {
	ms := s.enableChasmCallbacks(2)
	root, ok := ms.chasmTree.(*chasm.Node)
	s.True(ok)

	s.NoError(ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(2),
	}))
	err := ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(3),
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)

	s.False(root.IsDirty())
}

// The aggregate validator only governs callbacks attached to the CHASM tree. An execution whose
// callbacks go to the HSM tree keeps the HSM limit and must not consult it, even when CHASM
// itself is enabled.
func (s *mutableStateSuite) TestHsmCompletionCallbacks_AggregateValidationNotApplied() {
	ms := s.enableChasmCallbacks(1)
	s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)

	s.NoError(ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(3),
	}))

	s.startWithCompletionCallbacks(ms, 3)
	s.Equal(3, callbacks.MachineCollection(ms.HSM()).Size())
	wf := s.chasmWorkflowComponent(ms)
	s.Empty(wf.Callbacks)
	s.Zero(wf.GetCallbackMetadata().GetTotalCallbacksCount())
}

// An Update's callbacks are validated when the Update is admitted, before it reaches a worker.
// Once the worker has accepted it, its callbacks are attached as-is: rejecting them then would
// fail the entire workflow task completion, not just the Update. Nor are callbacks reapplied by
// reset or NDC conflict resolution checked again.
func (s *mutableStateSuite) TestChasmUpdateCallbacks_AttachingDoesNotValidate() {
	ms := s.enableChasmCallbacks(10)
	s.startWithCompletionCallbacks(ms, 0)
	_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.lowerCallbackLimits()

	updateRequest := func(updateID string) *updatepb.Request {
		return &updatepb.Request{
			Meta:                &updatepb.Meta{UpdateId: updateID},
			RequestId:           "req-" + updateID,
			CompletionCallbacks: testCompletionCallbacks(3),
		}
	}
	_, err = ms.AddWorkflowExecutionUpdateAcceptedEvent("accepted", "msg-id", 1, updateRequest("accepted"))
	s.NoError(err)
	_, err = ms.AddWorkflowExecutionUpdateAdmittedEvent(updateRequest("reapplied"), enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY)
	s.NoError(err)
	_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(
		nil, false, "req-attach", testCompletionCallbacks(3), nil, "", nil, nil, false, nil,
	)
	s.NoError(err)

	wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
	s.NoError(err)
	s.Len(wf.Callbacks, 3)
	s.Len(wf.Updates["accepted"].Get(ctx).Callbacks, 3)
	s.Len(wf.Updates["reapplied"].Get(ctx).Callbacks, 3)
	s.Equal(int64(9), wf.GetCallbackMetadata().GetTotalCallbacksCount())
}

// Callbacks of an Update in flight are reserved against the limits until it is accepted. Accepting
// it moves them into the persisted totals, after which the Registry no longer reports them, so
// they are counted exactly once throughout.
func (s *mutableStateSuite) TestChasmUpdateCallbacks_InFlightReservedUntilAccepted() {
	ms := s.enableChasmCallbacks(3)
	s.startWithCompletionCallbacks(ms, 1)
	_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)

	inFlight := chasmworkflow.CallbackAddition{UpdateID: "u1", RequestID: "req-u1", Callbacks: testCompletionCallbacks(2)}
	another := chasmworkflow.CallbackAddition{UpdateID: "u2", RequestID: "req-u2", Callbacks: testCompletionCallbacks(1)}
	var failedPrecondition *serviceerror.FailedPrecondition

	// One persisted and two in flight leave no room.
	err = ms.ValidateCallbackAddition([]chasmworkflow.CallbackAddition{inFlight}, another)
	s.ErrorAs(err, &failedPrecondition)
	s.ErrorContains(err, "(3 callbacks already attached)")
	// A retry of the in-flight request is not counted twice.
	s.NoError(ms.ValidateCallbackAddition([]chasmworkflow.CallbackAddition{inFlight}, inFlight))

	_, err = ms.AddWorkflowExecutionUpdateAcceptedEvent(inFlight.UpdateID, "msg-id", 1, &updatepb.Request{
		Meta:                &updatepb.Meta{UpdateId: inFlight.UpdateID},
		RequestId:           inFlight.RequestID,
		CompletionCallbacks: inFlight.Callbacks,
	})
	s.NoError(err)
	wf := s.chasmWorkflowComponent(ms)
	s.Equal(int64(3), wf.GetCallbackMetadata().GetTotalCallbacksCount())

	err = ms.ValidateCallbackAddition(nil, another)
	s.ErrorAs(err, &failedPrecondition)
	s.ErrorContains(err, "(3 callbacks already attached)")
	// Were the Registry still to report it, the persisted copy is recognized and not recounted.
	err = ms.ValidateCallbackAddition([]chasmworkflow.CallbackAddition{inFlight}, another)
	s.ErrorContains(err, "(3 callbacks already attached)")
}

func (s *mutableStateSuite) TestResultSubscriptionCallbacks_CancelAndReplay() {
	makeSubCB := func(subReqID, resolvedRunID string) *commonpb.Callback {
		return &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "temporal://system"},
			},
			ResultSubscription: &commonpb.Callback_ResultSubscription{
				RequestId:           subReqID,
				ResultFormatVersion: 1,
				ResolvedExecution: &commonpb.WorkflowExecution{
					WorkflowId: tests.WorkflowID,
					RunId:      resolvedRunID,
				},
			},
		}
	}

	s.Run("CHASM cancel, CAN inheritance, close, and replay under lowered limits", func() {
		ms := s.enableChasmCallbacks(10)
		_, completedEvent := s.startWithCompletionCallbacks(ms, 0)

		attachEv1, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
			nil, false, "sub-1", []*commonpb.Callback{makeSubCB("sub-1", tests.RunID)}, nil, "", nil, nil, false, nil,
		)
		s.Require().NoError(err)
		attachEv2, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
			nil, false, "sub-2", []*commonpb.Callback{makeSubCB("sub-2", tests.RunID)}, nil, "", nil, nil, false, nil,
		)
		s.Require().NoError(err)

		// Continue-as-new copies both subscriptions with ResultSubscription intact under new map keys.
		_, newRun, err := ms.AddContinueAsNewEvent(
			s.T().Context(),
			completedEvent.GetEventId(),
			"",
			&commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowRunTimeout: ms.GetExecutionInfo().WorkflowRunTimeout,
			},
			nil,
		)
		s.Require().NoError(err)

		// Cancel sub-1 on the new run via WorkflowExecutionOptionsUpdatedEvent.
		cancelEv, err := newRun.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
			&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				CanceledResultSubscriptionRequestIds: []string{"sub-1"},
			},
			nil,
		)
		s.Require().NoError(err)
		s.True(cancelEv.GetWorkerMayIgnore())

		wf, ctx, err := newRun.ChasmWorkflowComponentReadOnly(s.T().Context())
		s.Require().NoError(err)
		s.Equal(int64(2), wf.GetCallbackMetadata().GetTotalCallbacksCount())
		cb1, ok := wf.FindResultSubscription(ctx, "sub-1")
		s.Require().True(ok)
		s.Equal(callbackspb.CALLBACK_STATUS_FAILED, cb1.Status)
		s.NotNil(cb1.GetLastAttemptFailure().GetCanceledFailureInfo())
		cb2, ok := wf.FindResultSubscription(ctx, "sub-2")
		s.Require().True(ok)
		s.Equal(callbackspb.CALLBACK_STATUS_STANDBY, cb2.Status)

		// Replay both attach events and the cancel event on a fresh mutable state with lowered limits.
		replayMS := s.enableChasmCallbacks(0)
		s.lowerCallbackLimits()
		s.Require().NoError(replayMS.ApplyWorkflowExecutionOptionsUpdatedEvent(attachEv1))
		s.Require().NoError(replayMS.ApplyWorkflowExecutionOptionsUpdatedEvent(attachEv2))
		s.Require().NoError(replayMS.ApplyWorkflowExecutionOptionsUpdatedEvent(cancelEv))

		replayWF, replayCtx, err := replayMS.ChasmWorkflowComponentReadOnly(s.T().Context())
		s.Require().NoError(err)
		rcb1, ok := replayWF.FindResultSubscription(replayCtx, "sub-1")
		s.Require().True(ok)
		s.Equal(callbackspb.CALLBACK_STATUS_FAILED, rcb1.Status)
		s.NotNil(rcb1.GetLastAttemptFailure().GetCanceledFailureInfo())
		rcb2, ok := replayWF.FindResultSubscription(replayCtx, "sub-2")
		s.Require().True(ok)
		s.Equal(callbackspb.CALLBACK_STATUS_STANDBY, rcb2.Status)
	})

	s.Run("HSM cancel preserves other waiter, skips canceled on CAN, and does not fail workflow close", func() {
		ms := s.enableChasmCallbacks(10)
		s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
		_, completedEvent := s.startWithCompletionCallbacks(ms, 0)

		_, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
			nil, false, "sub-1", []*commonpb.Callback{makeSubCB("sub-1", tests.RunID)}, nil, "", nil, nil, false, nil,
		)
		s.Require().NoError(err)
		_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(
			nil, false, "sub-2", []*commonpb.Callback{makeSubCB("sub-2", tests.RunID)}, nil, "", nil, nil, false, nil,
		)
		s.Require().NoError(err)

		_, err = ms.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
			&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				CanceledResultSubscriptionRequestIds: []string{"sub-1"},
			},
			nil,
		)
		s.Require().NoError(err)

		coll := callbacks.MachineCollection(ms.HSM())
		cb1, err := coll.Data("cb-sub-1-0")
		s.Require().NoError(err)
		s.Equal(enumsspb.CALLBACK_STATE_FAILED, cb1.State())
		s.NotNil(cb1.GetLastAttemptFailure().GetCanceledFailureInfo())
		cb2, err := coll.Data("cb-sub-2-0")
		s.Require().NoError(err)
		s.Equal(enumsspb.CALLBACK_STATE_STANDBY, cb2.State())

		// Continue-as-new carries only STANDBY callbacks (sub-2), never resurrecting canceled sub-1.
		carried, err := getCompletionCallbacksAsProtoSlice(s.T().Context(), ms, false)
		s.Require().NoError(err)
		s.Require().Len(carried, 1)
		s.Equal("sub-2", carried[0].GetResultSubscription().GetRequestId())

		// Closing the workflow must succeed, keeping sub-1 FAILED-canceled and scheduling sub-2.
		_, err = ms.AddCompletedWorkflowEvent(completedEvent.GetEventId(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, "")
		s.Require().NoError(err)

		cb1, err = coll.Data("cb-sub-1-0")
		s.Require().NoError(err)
		s.Equal(enumsspb.CALLBACK_STATE_FAILED, cb1.State())
		s.NotNil(cb1.GetLastAttemptFailure().GetCanceledFailureInfo())
		cb2, err = coll.Data("cb-sub-2-0")
		s.Require().NoError(err)
		s.Equal(enumsspb.CALLBACK_STATE_SCHEDULED, cb2.State())
	})
}

func makeTestResultSubscriptionCallback(subReqID, workflowID, resolvedRunID string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: "temporal://system"},
		},
		ResultSubscription: &commonpb.Callback_ResultSubscription{
			RequestId:           subReqID,
			ResultFormatVersion: 1,
			ResolvedExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      resolvedRunID,
			},
		},
	}
}

func (s *mutableStateSuite) setupCallbackModeMS(chasmMode bool, maxCallbacks int) *MutableStateImpl {
	ms := s.enableChasmCallbacks(maxCallbacks)
	s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(chasmMode)
	s.mockConfig.MaxCallbacksPerWorkflow = dynamicconfig.GetIntPropertyFnFilteredByNamespace(maxCallbacks)
	return ms
}

// TestResultSubscriptionCallbacks_CronCarryOverAndTriggerAgreement verifies DEC-6 across
// all cron close paths (completed, failed without retry, timed out without retry) for both
// HSM and CHASM: result subscriptions stay STANDBY on the old cron run, carry over to the
// next cron run preserving metadata and slot count, while legacy and update callbacks keep
// firing on the old run and are never copied to the next cron run.
func (s *mutableStateSuite) TestResultSubscriptionCallbacks_CronCarryOverAndTriggerAgreement() {
	type closeKind struct {
		name  string
		close func(ms *MutableStateImpl, wftCompletedID int64, nextRunID string) error
	}
	closeCases := []closeKind{
		{
			name: "cron_completed",
			close: func(ms *MutableStateImpl, wftCompletedID int64, nextRunID string) error {
				_, err := ms.AddCompletedWorkflowEvent(wftCompletedID, &commandpb.CompleteWorkflowExecutionCommandAttributes{}, nextRunID)
				return err
			},
		},
		{
			name: "cron_failed_no_retry",
			close: func(ms *MutableStateImpl, wftCompletedID int64, nextRunID string) error {
				_, err := ms.AddFailWorkflowEvent(
					wftCompletedID,
					enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
					&commandpb.FailWorkflowExecutionCommandAttributes{Failure: &failurepb.Failure{Message: "cron fail"}},
					nextRunID,
				)
				return err
			},
		},
		{
			name: "cron_timed_out_no_retry",
			close: func(ms *MutableStateImpl, _ int64, nextRunID string) error {
				_, err := ms.AddTimeoutWorkflowEvent(enumspb.RETRY_STATE_TIMEOUT, nextRunID)
				return err
			},
		},
	}

	for _, chasmMode := range []bool{true, false} {
		modeName := "HSM"
		if chasmMode {
			modeName = "CHASM"
		}
		for _, cc := range closeCases {
			s.Run(modeName+"_"+cc.name, func() {
				ctx := s.T().Context()
				ms := s.setupCallbackModeMS(chasmMode, 10)
				startEvent, wftCompleted := s.startWithCompletionCallbacks(ms, 1) // 1 legacy callback

				activeSub := makeTestResultSubscriptionCallback("sub-active", tests.WorkflowID, tests.RunID)
				canceledSub := makeTestResultSubscriptionCallback("sub-canceled", tests.WorkflowID, tests.RunID)
				_, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
					nil, false, "sub-active", []*commonpb.Callback{activeSub}, nil, "", nil, nil, false, nil,
				)
				s.Require().NoError(err)
				_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(
					nil, false, "sub-canceled", []*commonpb.Callback{canceledSub}, nil, "", nil, nil, false, nil,
				)
				s.Require().NoError(err)
				_, err = ms.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
					&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
						CanceledResultSubscriptionRequestIds: []string{"sub-canceled"},
					},
					nil,
				)
				s.Require().NoError(err)

				if chasmMode {
					_, err = ms.AddWorkflowExecutionUpdateAdmittedEvent(&updatepb.Request{
						Meta:                &updatepb.Meta{UpdateId: "upd-1"},
						RequestId:           "req-upd-1",
						CompletionCallbacks: testCompletionCallbacks(1),
					}, enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED)
					s.Require().NoError(err)
				}

				nextRunID := uuid.NewString()
				s.Require().NoError(cc.close(ms, wftCompleted.GetEventId(), nextRunID))

				// On the old cron run: legacy (and update) callbacks fire; active result subscription stays
				// STANDBY (never fires on a successor close); canceled stays FAILED.
				if chasmMode {
					wf, readCtx, err := ms.ChasmWorkflowComponentReadOnly(ctx)
					s.Require().NoError(err)

					activeCB, ok := wf.FindResultSubscription(readCtx, "sub-active")
					s.Require().True(ok)
					s.Equal(callbackspb.CALLBACK_STATUS_STANDBY, activeCB.Status)

					canceledCB, ok := wf.FindResultSubscription(readCtx, "sub-canceled")
					s.Require().True(ok)
					s.Equal(callbackspb.CALLBACK_STATUS_FAILED, canceledCB.Status)
					s.NotNil(canceledCB.GetLastAttemptFailure().GetCanceledFailureInfo())

					s.Equal(callbackspb.CALLBACK_STATUS_SCHEDULED, wf.Callbacks["req-1-0"].Get(readCtx).Status)
					s.Equal(callbackspb.CALLBACK_STATUS_SCHEDULED, wf.Updates["upd-1"].Get(readCtx).Callbacks["req-upd-1-0"].Get(readCtx).Status)
				} else {
					coll := callbacks.MachineCollection(ms.HSM())
					activeCB, err := coll.Data("cb-sub-active-0")
					s.Require().NoError(err)
					s.Equal(enumsspb.CALLBACK_STATE_STANDBY, activeCB.State())

					canceledCB, err := coll.Data("cb-sub-canceled-0")
					s.Require().NoError(err)
					s.Equal(enumsspb.CALLBACK_STATE_FAILED, canceledCB.State())
					s.NotNil(canceledCB.GetLastAttemptFailure().GetCanceledFailureInfo())

					var legacyState enumsspb.CallbackState
					for _, node := range coll.List() {
						cb, err := coll.Data(node.Key.ID)
						s.Require().NoError(err)
						if cb.GetCallback().GetResultSubscription() == nil {
							legacyState = cb.State()
						}
					}
					s.Equal(enumsspb.CALLBACK_STATE_SCHEDULED, legacyState)
				}

				// Create the next cron run via SetupNewWorkflowForRetryOrCron.
				newRunEventsCache := events.NewMockCache(s.controller)
				newRunEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
				newRun, err := NewMutableStateInChain(
					s.mockShard, newRunEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, nextRunID, time.Now().UTC(), ms,
				)
				s.Require().NoError(err)
				s.Require().NoError(SetupNewWorkflowForRetryOrCron(
					ctx,
					ms,
					newRun,
					nextRunID,
					startEvent.GetWorkflowExecutionStartedEventAttributes(),
					nil,
					nil,
					nil,
					time.Minute,
					enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
				))

				// Next cron run carries ONLY the single active result subscription with original metadata and 1 slot.
				firstRunID, err := newRun.GetFirstRunID(ctx)
				s.Require().NoError(err)
				s.Equal(tests.RunID, firstRunID)

				if chasmMode {
					wf, readCtx, err := newRun.ChasmWorkflowComponentReadOnly(ctx)
					s.Require().NoError(err)
					s.Len(wf.Callbacks, 1)
					s.Equal(int64(1), wf.GetCallbackMetadata().GetTotalCallbacksCount())
					carried, ok := wf.FindResultSubscription(readCtx, "sub-active")
					s.Require().True(ok)
					s.Equal(callbackspb.CALLBACK_STATUS_STANDBY, carried.Status)
					protorequire.ProtoEqual(s.T(), activeSub.GetResultSubscription(), carried.GetCallback().GetResultSubscription())
					_, hasCanceled := wf.FindResultSubscription(readCtx, "sub-canceled")
					s.False(hasCanceled)
				} else {
					coll := callbacks.MachineCollection(newRun.HSM())
					s.Equal(1, coll.Size())
					nodes := coll.List()
					s.Require().Len(nodes, 1)
					carried, err := coll.Data(nodes[0].Key.ID)
					s.Require().NoError(err)
					s.Equal(enumsspb.CALLBACK_STATE_STANDBY, carried.State())
					protorequire.ProtoEqual(s.T(), activeSub.GetResultSubscription(), carried.GetCallback().GetResultSubscription())
				}

				// Terminal close on the new cron run fires the carried result subscription once and
				// GetResultSubscriptionCompletion produces the terminal outcome (no non-terminal error).
				termEv, err := newRun.AddTimeoutWorkflowEvent(enumspb.RETRY_STATE_TIMEOUT, "")
				s.Require().NoError(err)
				_, _, err = newRun.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive)
				s.Require().NoError(err)
				newRunEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(termEv, nil).AnyTimes()
				comp, err := newRun.GetResultSubscriptionCompletion(ctx, "cron-start-req", activeSub.GetResultSubscription())
				s.Require().NoError(err)
				s.NotNil(comp.Error)

				if chasmMode {
					wf, readCtx, err := newRun.ChasmWorkflowComponentReadOnly(ctx)
					s.Require().NoError(err)
					cb, ok := wf.FindResultSubscription(readCtx, "sub-active")
					s.Require().True(ok)
					s.Equal(callbackspb.CALLBACK_STATUS_SCHEDULED, cb.Status)
				} else {
					coll := callbacks.MachineCollection(newRun.HSM())
					cb, err := coll.Data(coll.List()[0].Key.ID)
					s.Require().NoError(err)
					s.Equal(enumsspb.CALLBACK_STATE_SCHEDULED, cb.State())
				}
			})
		}
	}
}

// TestResultSubscriptionCallbacks_DEC8CanceledNotCopiedAcrossRuns proves DEC-8 for both HSM and CHASM:
// within a single run a canceled subscription still occupies a slot, and across continue-as-new, retry,
// and cron the canceled subscription is NOT copied so its slot resets on the new run.
func (s *mutableStateSuite) TestResultSubscriptionCallbacks_DEC8CanceledNotCopiedAcrossRuns() {
	for _, chasmMode := range []bool{true, false} {
		modeName := "HSM"
		if chasmMode {
			modeName = "CHASM"
		}
		for _, transition := range []string{"continue_as_new", "retry_fail", "retry_timeout", "cron"} {
			s.Run(modeName+"_"+transition, func() {
				ctx := s.T().Context()
				ms := s.setupCallbackModeMS(chasmMode, 2)
				startEvent, wftCompleted := s.startWithCompletionCallbacks(ms, 0)

				sub1 := makeTestResultSubscriptionCallback("sub-active", tests.WorkflowID, tests.RunID)
				sub2 := makeTestResultSubscriptionCallback("sub-canceled", tests.WorkflowID, tests.RunID)
				_, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
					nil, false, "sub-active", []*commonpb.Callback{sub1}, nil, "", nil, nil, false, nil,
				)
				s.Require().NoError(err)
				_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(
					nil, false, "sub-canceled", []*commonpb.Callback{sub2}, nil, "", nil, nil, false, nil,
				)
				s.Require().NoError(err)
				_, err = ms.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
					&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
						CanceledResultSubscriptionRequestIds: []string{"sub-canceled"},
					},
					nil,
				)
				s.Require().NoError(err)

				// Within the old run, the canceled subscription still occupies its slot (2/2 full).
				if chasmMode {
					wf := s.chasmWorkflowComponent(ms)
					s.Equal(int64(2), wf.GetCallbackMetadata().GetTotalCallbacksCount())
					err = ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
						RequestID: "sub-extra",
						Callbacks: []*commonpb.Callback{makeTestResultSubscriptionCallback("sub-extra", tests.WorkflowID, tests.RunID)},
					})
					s.Error(err)
				} else {
					s.Equal(2, callbacks.MachineCollection(ms.HSM()).Size())
				}

				var newRun historyi.MutableState
				switch transition {
				case "continue_as_new":
					_, newRun, err = ms.AddContinueAsNewEvent(
						ctx,
						wftCompleted.GetEventId(),
						"",
						&commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
							WorkflowRunTimeout: ms.GetExecutionInfo().WorkflowRunTimeout,
						},
						nil,
					)
					s.Require().NoError(err)
				case "retry_fail", "retry_timeout", "cron":
					nextRunID := uuid.NewString()
					initiator := enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY
					switch transition {
					case "retry_fail":
						_, err = ms.AddFailWorkflowEvent(
							wftCompleted.GetEventId(),
							enumspb.RETRY_STATE_IN_PROGRESS,
							&commandpb.FailWorkflowExecutionCommandAttributes{Failure: &failurepb.Failure{Message: "retryable"}},
							nextRunID,
						)
					case "retry_timeout":
						_, err = ms.AddTimeoutWorkflowEvent(enumspb.RETRY_STATE_IN_PROGRESS, nextRunID)
					case "cron":
						initiator = enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
						_, err = ms.AddCompletedWorkflowEvent(
							wftCompleted.GetEventId(),
							&commandpb.CompleteWorkflowExecutionCommandAttributes{},
							nextRunID,
						)
					default:
						s.FailNowf("unexpected retry/cron transition", "%s", transition)
					}
					s.Require().NoError(err)

					newMS, err := NewMutableStateInChain(
						s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, nextRunID, time.Now().UTC(), ms,
					)
					s.Require().NoError(err)
					s.Require().NoError(SetupNewWorkflowForRetryOrCron(
						ctx,
						ms,
						newMS,
						nextRunID,
						startEvent.GetWorkflowExecutionStartedEventAttributes(),
						nil,
						nil,
						nil,
						time.Second,
						initiator,
					))
					newRun = newMS
				default:
					s.FailNowf("unexpected transition", "%s", transition)
				}

				// On the new run, only sub-active was copied; sub-canceled was NOT copied so 1 slot is free (1/2 used).
				if chasmMode {
					wf, readCtx, err := newRun.ChasmWorkflowComponentReadOnly(ctx)
					s.Require().NoError(err)
					s.Len(wf.Callbacks, 1)
					s.Equal(int64(1), wf.GetCallbackMetadata().GetTotalCallbacksCount())
					_, hasActive := wf.FindResultSubscription(readCtx, "sub-active")
					s.True(hasActive)
					_, hasCanceled := wf.FindResultSubscription(readCtx, "sub-canceled")
					s.False(hasCanceled)
					s.NoError(newRun.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
						RequestID: "sub-extra",
						Callbacks: []*commonpb.Callback{makeTestResultSubscriptionCallback("sub-extra", tests.WorkflowID, tests.RunID)},
					}))
				} else {
					coll := callbacks.MachineCollection(newRun.HSM())
					s.Equal(1, coll.Size())
					cb, err := coll.Data(coll.List()[0].Key.ID)
					s.Require().NoError(err)
					s.Equal("sub-active", cb.GetCallback().GetResultSubscription().GetRequestId())
				}
			})
		}
	}
}

// TestResultSubscriptionCallbacks_OldAttachCannotResurrectCanceled verifies that replaying an old
// attach event (both on the original run and on a successor run where the callback was re-keyed)
// cannot duplicate or resurrect a canceled subscription in either HSM or CHASM.
func (s *mutableStateSuite) TestResultSubscriptionCallbacks_OldAttachCannotResurrectCanceled() {
	for _, chasmMode := range []bool{true, false} {
		modeName := "HSM"
		if chasmMode {
			modeName = "CHASM"
		}
		s.Run(modeName, func() {
			ctx := s.T().Context()
			ms := s.setupCallbackModeMS(chasmMode, 10)
			_, wftCompleted := s.startWithCompletionCallbacks(ms, 0)

			sub := makeTestResultSubscriptionCallback("sub-1", tests.WorkflowID, tests.RunID)
			attachEv, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
				nil, false, "sub-1", []*commonpb.Callback{sub}, nil, "", nil, nil, false, nil,
			)
			s.Require().NoError(err)

			// 1. Inherit onto run 2 (re-keyed under run 2's start request ID), cancel on run 2,
			//    then replay run 1's old attach event onto run 2.
			_, run2, err := ms.AddContinueAsNewEvent(
				ctx,
				wftCompleted.GetEventId(),
				"",
				&commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowRunTimeout: ms.GetExecutionInfo().WorkflowRunTimeout,
				},
				nil,
			)
			s.Require().NoError(err)

			_, err = run2.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					CanceledResultSubscriptionRequestIds: []string{"sub-1"},
				},
				nil,
			)
			s.Require().NoError(err)

			// Replaying run 1's old attach event on run 2 must be a no-op (no duplicate, no resurrection).
			s.Require().NoError(run2.ApplyWorkflowExecutionOptionsUpdatedEvent(attachEv))

			// 2. Also cancel on a fresh run 1 and replay attachEv directly on run 1.
			ms1 := s.setupCallbackModeMS(chasmMode, 10)
			s.startWithCompletionCallbacks(ms1, 0)
			s.Require().NoError(ms1.ApplyWorkflowExecutionOptionsUpdatedEvent(attachEv))
			_, err = ms1.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					CanceledResultSubscriptionRequestIds: []string{"sub-1"},
				},
				nil,
			)
			s.Require().NoError(err)
			s.Require().NoError(ms1.ApplyWorkflowExecutionOptionsUpdatedEvent(attachEv))

			for _, targetMS := range []historyi.MutableState{run2, ms1} {
				if chasmMode {
					wf, readCtx, err := targetMS.ChasmWorkflowComponentReadOnly(ctx)
					s.Require().NoError(err)
					s.Len(wf.Callbacks, 1)
					s.Equal(int64(1), wf.GetCallbackMetadata().GetTotalCallbacksCount())
					cb, ok := wf.FindResultSubscription(readCtx, "sub-1")
					s.Require().True(ok)
					s.Equal(callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
					s.NotNil(cb.GetLastAttemptFailure().GetCanceledFailureInfo())
				} else {
					coll := callbacks.MachineCollection(targetMS.HSM())
					s.Equal(1, coll.Size())
					cb, err := coll.Data(coll.List()[0].Key.ID)
					s.Require().NoError(err)
					s.Equal(enumsspb.CALLBACK_STATE_FAILED, cb.State())
					s.NotNil(cb.GetLastAttemptFailure().GetCanceledFailureInfo())
				}
			}
		})
	}
}

// TestResultSubscriptionCallbacks_CacheEvictionReloadAndReplicationFailover exercises persistence
// reload (mutable-state cache eviction), history-based replication via MutableStateRebuilder
// (including cron carry-over and failover), and state-based replication via NewSanitizedMutableState
// in both HSM and CHASM modes.
func (s *mutableStateSuite) TestResultSubscriptionCallbacks_CacheEvictionReloadAndReplicationFailover() {
	for _, chasmMode := range []bool{true, false} {
		modeName := "HSM"
		if chasmMode {
			modeName = "CHASM"
		}
		s.Run(modeName, func() {
			ctx := s.T().Context()
			ms := s.setupCallbackModeMS(chasmMode, 10)

			// Configure a second cluster shard (standby at v1 -> promoted active at failoverVersion).
			const failoverVersion int64 = 2000
			standbyCfg := tests.NewDynamicConfig()
			standbyCfg.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
			standbyCfg.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(chasmMode)
			standbyCfg.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
			standbyCfg.MaxCallbacksPerWorkflow = dynamicconfig.GetIntPropertyFnFilteredByNamespace(10)
			standbyShard := shard.NewTestContext(s.controller, &persistencespb.ShardInfo{ShardId: 0, RangeId: 1}, standbyCfg)
			defer standbyShard.StopForTest()
			standbyHSMReg := hsm.NewRegistry()
			s.Require().NoError(RegisterStateMachine(standbyHSMReg))
			s.Require().NoError(callbacks.RegisterStateMachine(standbyHSMReg))
			s.Require().NoError(callbacks.RegisterTaskSerializers(standbyHSMReg))
			standbyShard.SetStateMachineRegistry(standbyHSMReg)
			standbyChasmReg := chasm.NewRegistry(log.NewTestLogger())
			s.Require().NoError(standbyChasmReg.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
			s.Require().NoError(standbyChasmReg.Register(chasmcallback.NewNilLibrary()))
			standbyShard.SetChasmRegistry(standbyChasmReg)
			standbyEventsCache := events.NewMockCache(s.controller)
			standbyEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
			standbyShard.SetEventsCacheForTesting(standbyEventsCache)

			standbyNS := namespace.NewGlobalNamespaceForTest(
				&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
				&persistencespb.NamespaceConfig{},
				&persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: cluster.TestCurrentClusterName,
					Clusters:          []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
				},
				tests.GlobalNamespaceEntry.FailoverVersion(tests.WorkflowID),
			)
			promotedActiveNS := namespace.NewGlobalNamespaceForTest(
				&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
				&persistencespb.NamespaceConfig{},
				&persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: cluster.TestAlternativeClusterName,
					Clusters:          []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
				},
				failoverVersion,
			)
			standbyShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(standbyNS, nil).AnyTimes()
			standbyShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()
			standbyShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(2)).AnyTimes()
			standbyShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).
				DoAndReturn(func(_ bool, version int64) string {
					if version == failoverVersion {
						return cluster.TestAlternativeClusterName
					}
					return cluster.TestCurrentClusterName
				}).AnyTimes()
			standbyShard.Resource.ClusterMetadata.EXPECT().IsVersionFromSameCluster(gomock.Any(), gomock.Any()).
				DoAndReturn(func(v1, v2 int64) bool { return v1 == v2 }).AnyTimes()
			s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, common.EmptyVersion).
				Return(cluster.TestCurrentClusterName).AnyTimes()

			startEv, wftCompleted := s.startWithCompletionCallbacks(ms, 1) // 1 legacy callback
			subActive := makeTestResultSubscriptionCallback("sub-active", tests.WorkflowID, tests.RunID)
			subCanceled := makeTestResultSubscriptionCallback("sub-canceled", tests.WorkflowID, tests.RunID)
			attachActiveEv, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
				nil, false, "sub-active", []*commonpb.Callback{subActive}, nil, "", nil, nil, false, nil,
			)
			s.Require().NoError(err)
			attachCanceledEv, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
				nil, false, "sub-canceled", []*commonpb.Callback{subCanceled}, nil, "", nil, nil, false, nil,
			)
			s.Require().NoError(err)
			cancelEv, err := ms.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					CanceledResultSubscriptionRequestIds: []string{"sub-canceled"},
				},
				nil,
			)
			s.Require().NoError(err)

			// --- 1. Cache eviction: snapshot -> NewMutableStateFromDB reload ---
			snap, evSeq, err := ms.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive)
			s.Require().NoError(err)
			var run1History []*historypb.HistoryEvent
			for _, batch := range evSeq {
				run1History = append(run1History, batch.Events...)
			}
			dbRecord := &persistencespb.WorkflowMutableState{
				ExecutionInfo:       common.CloneProto(snap.ExecutionInfo),
				ExecutionState:      common.CloneProto(snap.ExecutionState),
				NextEventId:         snap.NextEventID,
				ChasmNodes:          snap.ChasmNodes,
				ActivityInfos:       snap.ActivityInfos,
				TimerInfos:          snap.TimerInfos,
				ChildExecutionInfos: snap.ChildExecutionInfos,
				RequestCancelInfos:  snap.RequestCancelInfos,
				SignalInfos:         snap.SignalInfos,
				Checksum:            snap.Checksum,
			}

			reloaded, err := NewMutableStateFromDB(
				s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbRecord, 1,
			)
			s.Require().NoError(err)

			if chasmMode {
				wf, readCtx, err := reloaded.ChasmWorkflowComponentReadOnly(ctx)
				s.Require().NoError(err)
				s.Equal(int64(3), wf.GetCallbackMetadata().GetTotalCallbacksCount())
				cbA, ok := wf.FindResultSubscription(readCtx, "sub-active")
				s.Require().True(ok)
				s.Equal(callbackspb.CALLBACK_STATUS_STANDBY, cbA.Status)
				protorequire.ProtoEqual(s.T(), subActive.GetResultSubscription(), cbA.GetCallback().GetResultSubscription())
				cbC, ok := wf.FindResultSubscription(readCtx, "sub-canceled")
				s.Require().True(ok)
				s.Equal(callbackspb.CALLBACK_STATUS_FAILED, cbC.Status)
				s.NotNil(cbC.GetLastAttemptFailure().GetCanceledFailureInfo())
			} else {
				coll := callbacks.MachineCollection(reloaded.HSM())
				s.Equal(3, coll.Size())
				cbA, err := coll.Data("cb-sub-active-0")
				s.Require().NoError(err)
				s.Equal(enumsspb.CALLBACK_STATE_STANDBY, cbA.State())
				protorequire.ProtoEqual(s.T(), subActive.GetResultSubscription(), cbA.GetCallback().GetResultSubscription())
				cbC, err := coll.Data("cb-sub-canceled-0")
				s.Require().NoError(err)
				s.Equal(enumsspb.CALLBACK_STATE_FAILED, cbC.State())
				s.NotNil(cbC.GetLastAttemptFailure().GetCanceledFailureInfo())
			}

			// --- 2. State-based replication (NewSanitizedMutableState) ---
			sanitized, err := NewSanitizedMutableState(
				standbyShard, standbyEventsCache, s.logger, standbyNS, common.CloneProto(dbRecord),
				tests.GlobalNamespaceEntry.FailoverVersion(tests.WorkflowID),
			)
			s.Require().NoError(err)
			if chasmMode {
				wf, readCtx, err := sanitized.ChasmWorkflowComponentReadOnly(ctx)
				s.Require().NoError(err)
				cbA, ok := wf.FindResultSubscription(readCtx, "sub-active")
				s.Require().True(ok)
				s.Equal(callbackspb.CALLBACK_STATUS_STANDBY, cbA.Status)
				cbC, ok := wf.FindResultSubscription(readCtx, "sub-canceled")
				s.Require().True(ok)
				s.Equal(callbackspb.CALLBACK_STATUS_FAILED, cbC.Status)
			} else {
				coll := callbacks.MachineCollection(sanitized.HSM())
				cbA, err := coll.Data("cb-sub-active-0")
				s.Require().NoError(err)
				s.Equal(enumsspb.CALLBACK_STATE_STANDBY, cbA.State())
				cbC, err := coll.Data("cb-sub-canceled-0")
				s.Require().NoError(err)
				s.Equal(enumsspb.CALLBACK_STATE_FAILED, cbC.State())
			}

			// --- 3. History-based replication across cron (run 1 -> run 2) + failover ---
			s.Require().NoError(reloaded.UpdateCurrentVersion(s.namespaceEntry.FailoverVersion(tests.WorkflowID), true))
			run2ID := uuid.NewString()
			_, err = reloaded.AddCompletedWorkflowEvent(
				wftCompleted.GetEventId(),
				&commandpb.CompleteWorkflowExecutionCommandAttributes{},
				run2ID,
			)
			s.Require().NoError(err)
			_, closeSeq, err := reloaded.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
			s.Require().NoError(err)
			for _, batch := range closeSeq {
				run1History = append(run1History, batch.Events...)
			}

			run2Active, err := NewMutableStateInChain(
				s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, run2ID, time.Now().UTC(), reloaded,
			)
			s.Require().NoError(err)
			s.Require().NoError(SetupNewWorkflowForRetryOrCron(
				ctx,
				reloaded,
				run2Active,
				run2ID,
				startEv.GetWorkflowExecutionStartedEventAttributes(),
				nil,
				nil,
				nil,
				time.Minute,
				enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
			))
			_, run2EvSeq, err := run2Active.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive)
			s.Require().NoError(err)
			var run2History []*historypb.HistoryEvent
			for _, batch := range run2EvSeq {
				run2History = append(run2History, batch.Events...)
			}

			// Replicate run 1 + run 2 into passive standby mutable states via MutableStateRebuilder.
			standbyRun1 := NewMutableState(
				standbyShard, standbyEventsCache, s.logger, standbyNS, tests.WorkflowID, tests.RunID, time.Now().UTC(),
			)
			rebuilder := NewMutableStateRebuilder(standbyShard, s.logger, standbyRun1)
			var run1Batches [][]*historypb.HistoryEvent
			for _, ev := range run1History {
				run1Batches = append(run1Batches, []*historypb.HistoryEvent{ev})
			}
			standbyRun2, err := rebuilder.ApplyEvents(
				ctx,
				tests.NamespaceID,
				"repl-req",
				&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
				run1Batches,
				run2History,
				run2ID,
			)
			s.Require().NoError(err)
			s.Require().NotNil(standbyRun2)

			// Verify standby run 1: active result sub did NOT fire on cron close, canceled stayed FAILED, legacy fired.
			if chasmMode {
				wf1, readCtx1, err := standbyRun1.ChasmWorkflowComponentReadOnly(ctx)
				s.Require().NoError(err)
				cbA1, ok := wf1.FindResultSubscription(readCtx1, "sub-active")
				s.Require().True(ok)
				s.Equal(callbackspb.CALLBACK_STATUS_STANDBY, cbA1.Status)
				cbC1, ok := wf1.FindResultSubscription(readCtx1, "sub-canceled")
				s.Require().True(ok)
				s.Equal(callbackspb.CALLBACK_STATUS_FAILED, cbC1.Status)
				s.Equal(callbackspb.CALLBACK_STATUS_SCHEDULED, wf1.Callbacks["repl-req-0"].Get(readCtx1).Status)
			} else {
				coll1 := callbacks.MachineCollection(standbyRun1.HSM())
				cbA1, err := coll1.Data("cb-sub-active-0")
				s.Require().NoError(err)
				s.Equal(enumsspb.CALLBACK_STATE_STANDBY, cbA1.State())
				cbC1, err := coll1.Data("cb-sub-canceled-0")
				s.Require().NoError(err)
				s.Equal(enumsspb.CALLBACK_STATE_FAILED, cbC1.State())
			}

			// Persist passive standbyRun2 and reload under the promoted active namespace (failover).
			standbyRun2Snap, _, err := standbyRun2.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive)
			s.Require().NoError(err)
			promotedRun2, err := NewMutableStateFromDB(
				standbyShard, standbyEventsCache, s.logger, promotedActiveNS,
				&persistencespb.WorkflowMutableState{
					ExecutionInfo:  common.CloneProto(standbyRun2Snap.ExecutionInfo),
					ExecutionState: common.CloneProto(standbyRun2Snap.ExecutionState),
					NextEventId:    standbyRun2Snap.NextEventID,
					ChasmNodes:     standbyRun2Snap.ChasmNodes,
					Checksum:       standbyRun2Snap.Checksum,
				},
				1,
			)
			s.Require().NoError(err)
			s.Require().NoError(promotedRun2.UpdateCurrentVersion(failoverVersion, false))
			firstRunID, err := promotedRun2.GetFirstRunID(ctx)
			s.Require().NoError(err)
			s.Equal(tests.RunID, firstRunID)

			// Post-failover cancel of sub-active on promotedRun2 + replay of old attach events cannot resurrect.
			_, err = promotedRun2.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					CanceledResultSubscriptionRequestIds: []string{"sub-active"},
				},
				nil,
			)
			s.Require().NoError(err)
			s.Require().NoError(promotedRun2.ApplyWorkflowExecutionOptionsUpdatedEvent(attachActiveEv))
			s.Require().NoError(standbyRun1.ApplyWorkflowExecutionOptionsUpdatedEvent(attachCanceledEv))
			s.Require().NoError(standbyRun1.ApplyWorkflowExecutionOptionsUpdatedEvent(cancelEv))

			if chasmMode {
				wf2, readCtx2, err := promotedRun2.ChasmWorkflowComponentReadOnly(ctx)
				s.Require().NoError(err)
				s.Len(wf2.Callbacks, 1)
				s.Equal(int64(1), wf2.GetCallbackMetadata().GetTotalCallbacksCount())
				cbA2, ok := wf2.FindResultSubscription(readCtx2, "sub-active")
				s.Require().True(ok)
				s.Equal(callbackspb.CALLBACK_STATUS_FAILED, cbA2.Status)
				s.NotNil(cbA2.GetLastAttemptFailure().GetCanceledFailureInfo())
				protorequire.ProtoEqual(s.T(), subActive.GetResultSubscription(), cbA2.GetCallback().GetResultSubscription())
				_, hasCanceled := wf2.FindResultSubscription(readCtx2, "sub-canceled")
				s.False(hasCanceled)
			} else {
				coll2 := callbacks.MachineCollection(promotedRun2.HSM())
				s.Equal(1, coll2.Size())
				cbA2, err := coll2.Data(coll2.List()[0].Key.ID)
				s.Require().NoError(err)
				s.Equal(enumsspb.CALLBACK_STATE_FAILED, cbA2.State())
				s.NotNil(cbA2.GetLastAttemptFailure().GetCanceledFailureInfo())
				protorequire.ProtoEqual(s.T(), subActive.GetResultSubscription(), cbA2.GetCallback().GetResultSubscription())
			}
		})
	}
}
