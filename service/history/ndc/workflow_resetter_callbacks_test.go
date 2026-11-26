package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
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
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/callbacks"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newChasmCallbacksMutableState returns a running workflow whose completion callbacks are held
// by the CHASM tree, on a shard whose aggregate callback limits are far below what the tests
// reapply.
func newChasmCallbacksMutableState(t *testing.T) *workflow.MutableStateImpl {
	t.Helper()

	ctrl := gomock.NewController(t)
	config := tests.NewDynamicConfig()
	config.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	shardCtx := shard.NewTestContext(ctrl, &persistencespb.ShardInfo{ShardId: 1, RangeId: 1}, config)
	t.Cleanup(shardCtx.StopForTest)

	smRegistry := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(smRegistry))
	require.NoError(t, callbacks.RegisterStateMachine(smRegistry))
	require.NoError(t, callbacks.RegisterTaskSerializers(smRegistry))
	shardCtx.SetStateMachineRegistry(smRegistry)
	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, chasmRegistry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
	require.NoError(t, chasmRegistry.Register(chasmcallback.NewNilLibrary()))
	shardCtx.SetChasmRegistry(chasmRegistry)

	validatorConfig := test.NewCallbacksValidatorConfig()
	validatorConfig.MaxCallbacksPerExecution = func(string) int { return 1 }
	validatorConfig.TotalCallbacksMaxSize = func(string) int { return 1 }
	shardCtx.SetCallbackValidator(test.NewCallbacksValidator(t, validatorConfig))

	namespaceEntry := tests.GlobalNamespaceEntry
	shardCtx.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
		Return(cluster.TestCurrentClusterName).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	eventsCache := events.NewMockCache(ctrl)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	shardCtx.SetEventsCacheForTesting(eventsCache)

	ms := workflow.NewMutableState(
		shardCtx, eventsCache, shardCtx.GetLogger(), namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)
	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:    "req-start",
				WorkflowType: &commonpb.WorkflowType{Name: "test-workflow-type"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			},
		},
	)
	require.NoError(t, err)
	return ms
}

func testNexusCallbacks(n int) []*commonpb.Callback {
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

// Reset and NDC conflict resolution reapply events whose callbacks were validated when first
// attached. Even though they now breach both the execution-wide and per-update limits, they
// must be reapplied as-is rather than failing the reset or stalling replication.
func TestReapplyEvents_KeepsCallbacksOverLimit(t *testing.T) {
	t.Parallel()

	const updateID = "update-id"
	reappliedEvents := []*historypb.HistoryEvent{
		{
			EventId:   10,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
				WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					AttachedRequestId:           "req-attach",
					AttachedCompletionCallbacks: testNexusCallbacks(3),
				},
			},
		},
		{
			EventId:   11,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{
				WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
					Request: &updatepb.Request{
						Meta:                &updatepb.Meta{UpdateId: updateID},
						RequestId:           "req-update",
						CompletionCallbacks: testNexusCallbacks(3),
					},
				},
			},
		},
	}

	for _, tc := range []struct {
		name    string
		isReset bool
	}{
		{name: "Reset", isReset: true},
		{name: "ConflictResolution", isReset: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ms := newChasmCallbacksMutableState(t)

			// Conflict resolution passes the target branch's update registry and dedups against the
			// losing run; reset does neither.
			var targetBranchUpdateRegistry update.Registry
			runIDForDeduplication := ""
			if !tc.isReset {
				targetBranchUpdateRegistry = update.NewRegistry(ms)
				runIDForDeduplication = "losing-run-id"
			}

			applied, err := reapplyEvents(
				context.Background(),
				ms,
				targetBranchUpdateRegistry,
				hsm.NewRegistry(),
				chasmworkflow.NewRegistry(),
				reappliedEvents,
				nil,
				runIDForDeduplication,
				tc.isReset,
				log.NewTestLogger(),
			)
			require.NoError(t, err)
			require.Len(t, applied, len(reappliedEvents))

			wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
			require.NoError(t, err)
			require.Len(t, wf.Callbacks, 3)
			require.Contains(t, wf.Updates, updateID)
			require.Len(t, wf.Updates[updateID].Get(ctx).Callbacks, 3)
			require.Equal(t, int64(6), wf.GetCallbackMetadata().GetTotalCallbacksCount())
		})
	}
}

func newCallbacksTestMutableState(t *testing.T, chasmMode bool, startCallbacks ...*commonpb.Callback) (*workflow.MutableStateImpl, *shard.ContextTest) {
	t.Helper()

	ctrl := gomock.NewController(t)
	config := tests.NewDynamicConfig()
	config.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(chasmMode)
	config.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.MaxCallbacksPerWorkflow = dynamicconfig.GetIntPropertyFnFilteredByNamespace(10)

	shardCtx := shard.NewTestContext(ctrl, &persistencespb.ShardInfo{ShardId: 1, RangeId: 1}, config)
	t.Cleanup(shardCtx.StopForTest)

	smRegistry := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(smRegistry))
	require.NoError(t, callbacks.RegisterStateMachine(smRegistry))
	require.NoError(t, callbacks.RegisterTaskSerializers(smRegistry))
	shardCtx.SetStateMachineRegistry(smRegistry)
	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, chasmRegistry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
	require.NoError(t, chasmRegistry.Register(chasmcallback.NewNilLibrary()))
	shardCtx.SetChasmRegistry(chasmRegistry)

	validatorConfig := test.NewCallbacksValidatorConfig()
	validatorConfig.MaxCallbacksPerExecution = func(string) int { return 10 }
	shardCtx.SetCallbackValidator(test.NewCallbacksValidator(t, validatorConfig))

	namespaceEntry := tests.GlobalNamespaceEntry
	shardCtx.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
		Return(cluster.TestCurrentClusterName).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	eventsCache := events.NewMockCache(ctrl)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	shardCtx.SetEventsCacheForTesting(eventsCache)

	ms := workflow.NewMutableState(
		shardCtx, eventsCache, shardCtx.GetLogger(), namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)
	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "req-start",
				WorkflowType:        &commonpb.WorkflowType{Name: "test-workflow-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				CompletionCallbacks: startCallbacks,
			},
		},
	)
	require.NoError(t, err)
	return ms, shardCtx
}

func requireResultSubCanceled(t *testing.T, ms *workflow.MutableStateImpl, chasmMode bool, reqID string, wantSub *commonpb.Callback_ResultSubscription) {
	t.Helper()
	if chasmMode {
		wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
		require.NoError(t, err)
		require.Len(t, wf.Callbacks, 1)
		require.Equal(t, int64(1), wf.GetCallbackMetadata().GetTotalCallbacksCount())
		cb, ok := wf.FindResultSubscription(ctx, reqID)
		require.True(t, ok)
		require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
		require.NotNil(t, cb.GetLastAttemptFailure().GetCanceledFailureInfo())
		protorequire.ProtoEqual(t, wantSub, cb.GetCallback().GetResultSubscription())
		return
	}
	coll := callbacks.MachineCollection(ms.HSM())
	require.Equal(t, 1, coll.Size())
	cb, err := coll.Data(coll.List()[0].Key.ID)
	require.NoError(t, err)
	require.Equal(t, enumsspb.CALLBACK_STATE_FAILED, cb.State())
	require.NotNil(t, cb.GetLastAttemptFailure().GetCanceledFailureInfo())
	protorequire.ProtoEqual(t, wantSub, cb.GetCallback().GetResultSubscription())
}

func TestReapplyEvents_ResultSubscriptionCancelSurvivesReset(t *testing.T) {
	t.Parallel()

	subCB := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: "temporal://system"},
		},
		ResultSubscription: &commonpb.Callback_ResultSubscription{
			RequestId:           "sub-req-1",
			ResultFormatVersion: 1,
			ResolvedExecution: &commonpb.WorkflowExecution{
				WorkflowId: tests.WorkflowID,
				RunId:      tests.RunID,
			},
		},
	}
	attachEvent := &historypb.HistoryEvent{
		EventId:   10,
		Version:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
			WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				AttachedRequestId:           "sub-req-1",
				AttachedCompletionCallbacks: []*commonpb.Callback{subCB},
			},
		},
	}
	cancelEvent := &historypb.HistoryEvent{
		EventId:   11,
		Version:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
			WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				CanceledResultSubscriptionRequestIds: []string{"sub-req-1"},
			},
		},
	}

	for _, chasmMode := range []bool{true, false} {
		modeName := "HSM"
		if chasmMode {
			modeName = "CHASM"
		}

		t.Run(modeName+"_reset_to_before_attach_reapplies_attach_and_cancel_leaving_callback_canceled", func(t *testing.T) {
			t.Parallel()

			ms, _ := newCallbacksTestMutableState(t, chasmMode)
			applied, err := reapplyEvents(
				t.Context(),
				ms,
				nil,
				hsm.NewRegistry(),
				chasmworkflow.NewRegistry(),
				[]*historypb.HistoryEvent{attachEvent, cancelEvent},
				nil,
				"",
				true,
				log.NewTestLogger(),
			)
			require.NoError(t, err)
			require.Len(t, applied, 2)

			requireResultSubCanceled(t, ms, chasmMode, "sub-req-1", subCB.GetResultSubscription())
		})

		t.Run(modeName+"_reset_after_attach_reapplies_cancel_only_event_without_dropping_it", func(t *testing.T) {
			t.Parallel()

			ms, _ := newCallbacksTestMutableState(t, chasmMode)
			// Base branch before reset point already has the attachment applied.
			_, err := ms.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
				attachEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				nil,
			)
			require.NoError(t, err)

			// Reapplying both events skips the duplicate attach and applies the cancel-only event.
			applied, err := reapplyEvents(
				t.Context(),
				ms,
				nil,
				hsm.NewRegistry(),
				chasmworkflow.NewRegistry(),
				[]*historypb.HistoryEvent{attachEvent, cancelEvent},
				nil,
				"",
				true,
				log.NewTestLogger(),
			)
			require.NoError(t, err)
			require.Len(t, applied, 1)
			require.Equal(t, cancelEvent.GetEventId(), applied[0].GetEventId())

			requireResultSubCanceled(t, ms, chasmMode, "sub-req-1", subCB.GetResultSubscription())
		})

		t.Run(modeName+"_reapplying_old_attach_after_cancel_on_inherited_run_does_not_resurrect", func(t *testing.T) {
			t.Parallel()

			// Inherited run carries subCB on WorkflowExecutionStarted (keyed by start request ID, not "sub-req-1").
			ms, _ := newCallbacksTestMutableState(t, chasmMode, subCB)
			_, err := ms.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
				cancelEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				nil,
			)
			require.NoError(t, err)

			// Reapplying an old attach event from the earlier run must not duplicate or resurrect the canceled callback.
			_, err = reapplyEvents(
				t.Context(),
				ms,
				nil,
				hsm.NewRegistry(),
				chasmworkflow.NewRegistry(),
				[]*historypb.HistoryEvent{attachEvent},
				nil,
				"",
				true,
				log.NewTestLogger(),
			)
			require.NoError(t, err)
			requireResultSubCanceled(t, ms, chasmMode, "sub-req-1", subCB.GetResultSubscription())
		})

		t.Run(modeName+"_state_rebuilder_rebuild_and_reapply_preserves_cancel_and_metadata", func(t *testing.T) {
			t.Parallel()

			_, shardCtx := newCallbacksTestMutableState(t, chasmMode)
			now := time.Now().UTC()
			branchToken := []byte("base-branch")
			targetBranchToken := []byte("reset-branch")
			baseRunID := "00000000-0000-4000-8000-000000000011"
			resetRunID := "00000000-0000-4000-8000-000000000022"
			subForBase := &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: "temporal://system"},
				},
				ResultSubscription: &commonpb.Callback_ResultSubscription{
					RequestId:           "sub-req-1",
					ResultFormatVersion: 1,
					ResolvedExecution: &commonpb.WorkflowExecution{
						WorkflowId: tests.WorkflowID,
						RunId:      baseRunID,
					},
				},
			}
			baseAttachEv := &historypb.HistoryEvent{
				EventId:   2,
				Version:   1,
				EventTime: timestamppb.New(now),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
					WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
						AttachedRequestId:           "sub-req-1",
						AttachedCompletionCallbacks: []*commonpb.Callback{subForBase},
					},
				},
			}
			historyBatches := []*historypb.History{
				{
					Events: []*historypb.HistoryEvent{
						{
							EventId:   1,
							Version:   1,
							EventTime: timestamppb.New(now),
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
							Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
								WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
									WorkflowType:        &commonpb.WorkflowType{Name: "test-workflow-type"},
									TaskQueue:           &taskqueuepb.TaskQueue{Name: "test-task-queue"},
									FirstExecutionRunId: baseRunID,
								},
							},
						},
						baseAttachEv,
					},
				},
			}
			shardCtx.Resource.ExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(
				&persistence.ReadHistoryBranchByBatchResponse{
					History:        historyBatches,
					TransactionIDs: []int64{10},
				},
				nil,
			)

			rebuilder := NewStateRebuilder(shardCtx, shardCtx.GetLogger())
			ctrl := gomock.NewController(t)
			mockRefresher := workflow.NewMockTaskRefresher(ctrl)
			mockRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			rebuilder.taskRefresher = mockRefresher

			rebuilt, _, err := rebuilder.Rebuild(
				t.Context(),
				now,
				definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, baseRunID),
				branchToken,
				2,
				new(int64(1)),
				definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, resetRunID),
				targetBranchToken,
				"reset-req-id",
			)
			require.NoError(t, err)
			rebuiltImpl, ok := rebuilt.(*workflow.MutableStateImpl)
			require.True(t, ok)

			// Reapply post-reset cancel plus a duplicate old attach event.
			_, err = reapplyEvents(
				t.Context(),
				rebuiltImpl,
				nil,
				shardCtx.StateMachineRegistry(),
				chasmworkflow.NewRegistry(),
				[]*historypb.HistoryEvent{cancelEvent, baseAttachEv},
				nil,
				"",
				true,
				log.NewTestLogger(),
			)
			require.NoError(t, err)
			requireResultSubCanceled(t, rebuiltImpl, chasmMode, "sub-req-1", subForBase.GetResultSubscription())

			firstRunID, err := rebuiltImpl.GetFirstRunID(t.Context())
			require.NoError(t, err)
			require.Equal(t, baseRunID, firstRunID)
		})
	}
}
