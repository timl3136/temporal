package getworkflowexecutionresult

import (
	"context"
	"errors"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

const maxStaleCurrentRunAttempts = 5

type resolveRequest struct {
	namespaceID namespace.ID
	caller      *historyservice.NexusCallerInfo
	target      *commonpb.WorkflowExecution
}

type resolvedTarget struct {
	workflowKey definition.WorkflowKey
	firstRunID  string
	// closeEvent is non-nil when the resolved execution is terminal (closed with no successor).
	// It is cloned under the workflow lease before the lease is released.
	closeEvent *historypb.HistoryEvent
}

// resolveActionFunc runs under the resolved target's workflow lease.
// When target.closeEvent is non-nil, the target is closed and no persistence write is performed.
type resolveActionFunc func(lease api.WorkflowLease, target resolvedTarget) (*api.UpdateWorkflowAction, error)

// resolveTarget resolves req.target under its workflow lease and invokes action if non-nil.
// It returns either the resolved open execution (with its chain's first run ID) or a cloned
// terminal close event, without holding two run locks at once.
func resolveTarget(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	action resolveActionFunc,
) (resolvedTarget, error) {
	if err := validateResolveRequest(req); err != nil {
		return resolvedTarget{}, err
	}
	if req.target.GetRunId() == "" {
		return resolveCurrent(ctx, shard, checker, req, "", action)
	}
	return resolveExplicitRun(ctx, shard, checker, req, action)
}

func validateResolveRequest(req resolveRequest) error {
	if err := api.ValidateNamespaceUUID(req.namespaceID); err != nil {
		return err
	}
	if req.target == nil || req.target.GetWorkflowId() == "" {
		return consts.ErrWorkflowIDNotSet
	}
	if runID := req.target.GetRunId(); runID != "" && uuid.Validate(runID) != nil {
		return consts.ErrInvalidRunID
	}
	if req.caller == nil || req.caller.GetNamespaceId() == "" || req.caller.GetWorkflowId() == "" || req.caller.GetRunId() == "" {
		return serviceerror.NewInvalidArgument("caller workflow identity is required")
	}
	if req.caller.GetNamespaceId() != req.namespaceID.String() {
		return serviceerror.NewInvalidArgument("caller namespace does not match target namespace")
	}
	if req.caller.GetWorkflowId() == req.target.GetWorkflowId() &&
		(req.target.GetRunId() == "" || req.target.GetRunId() == req.caller.GetRunId()) {
		return newSelfWaitError()
	}
	return nil
}

func resolveExplicitRun(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	action resolveActionFunc,
) (resolvedTarget, error) {
	var lastErr error
	for range maxStaleCurrentRunAttempts {
		if err := ctx.Err(); err != nil {
			return resolvedTarget{}, err
		}
		resolved, followFirstRunID, err := resolveExplicitRunOnce(ctx, shard, checker, req, action)
		if err != nil {
			if isStaleCurrentRunError(err) {
				lastErr = err
				continue
			}
			return resolvedTarget{}, err
		}
		if followFirstRunID != "" {
			// The requested run's lease has been released before acquiring the current run's lease.
			return resolveCurrent(ctx, shard, checker, req, followFirstRunID, action)
		}
		return resolved, nil
	}
	return resolvedTarget{}, lastErr
}

func resolveExplicitRunOnce(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	action resolveActionFunc,
) (resolvedTarget, string, error) {
	var (
		resolved         resolvedTarget
		followFirstRunID string
		cleanErr         error
	)
	workflowKey := definition.NewWorkflowKey(
		req.namespaceID.String(),
		req.target.GetWorkflowId(),
		req.target.GetRunId(),
	)
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			ms := lease.GetMutableState()
			actualKey, err := resolvedWorkflowKey(req.namespaceID, req.target.GetWorkflowId(), lease)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			firstRunID, err := ms.GetFirstRunID(ctx)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			if ms.IsWorkflowExecutionRunning() {
				if isSelfWait(req.caller, actualKey, firstRunID) {
					cleanErr = newSelfWaitError()
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				if firstRunID == "" {
					cleanErr = serviceerror.NewInternal("workflow execution is missing first execution run ID")
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				resolved = resolvedTarget{
					workflowKey: actualKey,
					firstRunID:  firstRunID,
				}
				return runOpenAction(lease, resolved, action)
			}

			closeEvent, superseded, err := readTerminalCloseEvent(ctx, ms)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			if superseded {
				if firstRunID == "" {
					cleanErr = newDifferentChainError()
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				followFirstRunID = firstRunID
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			if isSelfWait(req.caller, actualKey, firstRunID) {
				cleanErr = newSelfWaitError()
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			resolved = resolvedTarget{
				workflowKey: actualKey,
				firstRunID:  firstRunID,
				closeEvent:  closeEvent,
			}
			return runClosedAction(lease, resolved, action)
		},
		nil,
		shard,
		checker,
	)
	if err != nil {
		return resolvedTarget{}, "", err
	}
	if cleanErr != nil {
		return resolvedTarget{}, "", cleanErr
	}
	return resolved, followFirstRunID, nil
}

func resolveCurrent(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	expectedFirstRunID string,
	action resolveActionFunc,
) (resolvedTarget, error) {
	var lastErr error
	for range maxStaleCurrentRunAttempts {
		if err := ctx.Err(); err != nil {
			return resolvedTarget{}, err
		}
		resolved, err := resolveCurrentOnce(ctx, shard, checker, req, expectedFirstRunID, action)
		if err != nil {
			if isStaleCurrentRunError(err) {
				lastErr = err
				continue
			}
			return resolvedTarget{}, err
		}
		return resolved, nil
	}
	return resolvedTarget{}, lastErr
}

func resolveCurrentOnce(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	expectedFirstRunID string,
	action resolveActionFunc,
) (resolvedTarget, error) {
	var (
		resolved resolvedTarget
		cleanErr error
	)
	workflowKey := definition.NewWorkflowKey(
		req.namespaceID.String(),
		req.target.GetWorkflowId(),
		"",
	)
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			ms := lease.GetMutableState()
			actualKey, err := resolvedWorkflowKey(req.namespaceID, req.target.GetWorkflowId(), lease)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			var firstRunID string
			if expectedFirstRunID != "" {
				firstRunID, err = ms.GetFirstRunID(ctx)
				if err != nil {
					cleanErr = err
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				if firstRunID == "" || firstRunID != expectedFirstRunID {
					cleanErr = newDifferentChainError()
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
			}

			if ms.IsWorkflowExecutionRunning() {
				if expectedFirstRunID == "" {
					firstRunID, err = ms.GetFirstRunID(ctx)
					if err != nil {
						cleanErr = err
						return &api.UpdateWorkflowAction{Noop: true}, nil
					}
				}
				if isSelfWait(req.caller, actualKey, firstRunID) {
					cleanErr = newSelfWaitError()
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				if firstRunID == "" {
					cleanErr = serviceerror.NewInternal("workflow execution is missing first execution run ID")
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				resolved = resolvedTarget{
					workflowKey: actualKey,
					firstRunID:  firstRunID,
				}
				return runOpenAction(lease, resolved, action)
			}

			closeEvent, superseded, err := readTerminalCloseEvent(ctx, ms)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			if superseded {
				cleanErr = consts.ErrLocateCurrentWorkflowExecution
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			if expectedFirstRunID == "" {
				firstRunID, err = ms.GetFirstRunID(ctx)
				if err != nil {
					cleanErr = err
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
			}
			if isSelfWait(req.caller, actualKey, firstRunID) {
				cleanErr = newSelfWaitError()
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			resolved = resolvedTarget{
				workflowKey: actualKey,
				firstRunID:  firstRunID,
				closeEvent:  closeEvent,
			}
			return runClosedAction(lease, resolved, action)
		},
		nil,
		shard,
		checker,
	)
	if err != nil {
		return resolvedTarget{}, err
	}
	if cleanErr != nil {
		return resolvedTarget{}, cleanErr
	}
	return resolved, nil
}

// readTerminalCloseEvent returns a cloned terminal close event for a closed run,
// or superseded=true if the run has a chain successor.
func readTerminalCloseEvent(
	ctx context.Context,
	ms historyi.MutableState,
) (*historypb.HistoryEvent, bool, error) {
	if hasMutableStateSuccessor(ms) {
		return nil, true, nil
	}
	completionEvent, err := ms.GetCompletionEvent(ctx)
	if err != nil {
		return nil, false, err
	}
	if completionEvent == nil {
		return nil, false, serviceerror.NewInternal("missing workflow completion event")
	}
	if hasCompletionEventSuccessor(completionEvent) {
		return nil, true, nil
	}
	return common.CloneProto(completionEvent), false, nil
}

func runOpenAction(
	lease api.WorkflowLease,
	resolved resolvedTarget,
	action resolveActionFunc,
) (*api.UpdateWorkflowAction, error) {
	if action == nil {
		return &api.UpdateWorkflowAction{Noop: true}, nil
	}
	postActions, err := action(lease, resolved)
	if err != nil {
		return nil, err
	}
	if postActions == nil {
		return &api.UpdateWorkflowAction{Noop: true}, nil
	}
	return postActions, nil
}

func runClosedAction(
	lease api.WorkflowLease,
	resolved resolvedTarget,
	action resolveActionFunc,
) (*api.UpdateWorkflowAction, error) {
	if action == nil {
		return &api.UpdateWorkflowAction{Noop: true}, nil
	}
	postActions, err := action(lease, resolved)
	if err != nil {
		return nil, err
	}
	if postActions != nil && !postActions.Noop {
		return nil, serviceerror.NewInternal("closed workflow execution cannot be mutated")
	}
	return &api.UpdateWorkflowAction{Noop: true}, nil
}

func resolvedWorkflowKey(
	namespaceID namespace.ID,
	workflowID string,
	lease api.WorkflowLease,
) (definition.WorkflowKey, error) {
	runID := lease.GetMutableState().GetExecutionState().GetRunId()
	if runID == "" {
		runID = lease.GetContext().GetWorkflowKey().RunID
	}
	if runID == "" {
		return definition.WorkflowKey{}, serviceerror.NewInternal("workflow execution is missing run ID")
	}
	return definition.NewWorkflowKey(namespaceID.String(), workflowID, runID), nil
}

func hasMutableStateSuccessor(ms historyi.MutableState) bool {
	info := ms.GetExecutionInfo()
	state := ms.GetExecutionState()
	runID := state.GetRunId()
	if info.GetNewExecutionRunId() != "" {
		return true
	}
	if state.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
		return true
	}
	if info.GetWorkflowWasReset() {
		return true
	}
	if resetRunID := info.GetResetRunId(); resetRunID != "" && resetRunID != runID {
		return true
	}
	return false
}

func hasCompletionEventSuccessor(event *historypb.HistoryEvent) bool {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return event.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId() != ""
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		return event.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId() != ""
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return event.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId() != ""
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return event.GetWorkflowExecutionTerminatedEventAttributes().GetIdentity() == consts.IdentityResetter
	default:
		return false
	}
}

func isSelfWait(
	caller *historyservice.NexusCallerInfo,
	resolvedKey definition.WorkflowKey,
	firstRunID string,
) bool {
	if caller.GetNamespaceId() != resolvedKey.NamespaceID || caller.GetWorkflowId() != resolvedKey.WorkflowID {
		return false
	}
	return resolvedKey.RunID == caller.GetRunId() ||
		(firstRunID != "" && firstRunID == caller.GetRunId())
}

func isStaleCurrentRunError(err error) bool {
	if errors.Is(err, consts.ErrLocateCurrentWorkflowExecution) {
		return true
	}
	_, isCurrentConflict := errors.AsType[*persistence.CurrentWorkflowConditionFailedError](err)
	return isCurrentConflict
}

func newSelfWaitError() error {
	return serviceerror.NewInvalidArgument("workflow execution cannot await its own result")
}

func newDifferentChainError() error {
	return serviceerror.NewFailedPrecondition("target workflow execution belongs to a different execution chain")
}
