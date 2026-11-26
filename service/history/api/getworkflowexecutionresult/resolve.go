package getworkflowexecutionresult

import (
	"context"
	"errors"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
)

const (
	maxStaleCurrentRunAttempts = 5
	// shortcut: chains longer than 100 runs fail after workflow ID reuse; raise if long continue-as-new chains need pinned waits.
	maxChainWalkHops = 100
)

var errDifferentChain = errors.New("target workflow execution belongs to a different execution chain")

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

type resolveStep struct {
	target    resolvedTarget
	follow    bool
	nextRunID string
}

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
	workflowKey := definition.NewWorkflowKey(
		req.namespaceID.String(),
		req.target.GetWorkflowId(),
		req.target.GetRunId(),
	)
	step, err := resolveRunWithRetry(ctx, shard, checker, req, workflowKey, "", true, action)
	if err != nil {
		return resolvedTarget{}, err
	}
	if !step.follow {
		return step.target, nil
	}
	// The requested run's lease has been released before acquiring the current run's lease.
	if step.target.firstRunID != "" {
		resolved, err := resolveCurrent(ctx, shard, checker, req, step.target.firstRunID, action)
		if err == nil {
			return resolved, nil
		}
		_, isNotFound := errors.AsType[*serviceerror.NotFound](err)
		if !errors.Is(err, errDifferentChain) && !isNotFound {
			return resolvedTarget{}, err
		}
	}
	// Current run belongs to a different chain (or is absent); walk forward from the requested run.
	return walkChain(ctx, shard, checker, req, workflowKey.RunID, step.nextRunID, action)
}

func resolveCurrent(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	expectedFirstRunID string,
	action resolveActionFunc,
) (resolvedTarget, error) {
	workflowKey := definition.NewWorkflowKey(
		req.namespaceID.String(),
		req.target.GetWorkflowId(),
		"",
	)
	step, err := resolveRunWithRetry(ctx, shard, checker, req, workflowKey, expectedFirstRunID, false, action)
	if err != nil {
		return resolvedTarget{}, err
	}
	return step.target, nil
}

func walkChain(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	startRunID string,
	nextRunID string,
	action resolveActionFunc,
) (resolvedTarget, error) {
	visited := map[string]struct{}{startRunID: {}}
	for range maxChainWalkHops {
		if nextRunID == "" {
			return resolvedTarget{}, serviceerror.NewFailedPrecondition("target workflow execution chain is broken: missing successor run")
		}
		if _, seen := visited[nextRunID]; seen {
			return resolvedTarget{}, serviceerror.NewFailedPrecondition("target workflow execution chain contains a cycle")
		}
		visited[nextRunID] = struct{}{}

		hopKey := definition.NewWorkflowKey(
			req.namespaceID.String(),
			req.target.GetWorkflowId(),
			nextRunID,
		)
		step, err := resolveRunWithRetry(ctx, shard, checker, req, hopKey, "", true, action)
		if err != nil {
			if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
				return resolvedTarget{}, serviceerror.NewFailedPrecondition("target workflow execution chain is broken: successor run not found")
			}
			return resolvedTarget{}, err
		}
		if !step.follow {
			return step.target, nil
		}
		nextRunID = step.nextRunID
	}
	return resolvedTarget{}, serviceerror.NewFailedPrecondition("target workflow execution chain exceeded maximum walk hops")
}

func resolveRunWithRetry(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	workflowKey definition.WorkflowKey,
	expectedFirstRunID string,
	explicit bool,
	action resolveActionFunc,
) (resolveStep, error) {
	var lastErr error
	for range maxStaleCurrentRunAttempts {
		if err := ctx.Err(); err != nil {
			return resolveStep{}, err
		}
		step, err := resolveOnce(ctx, shard, checker, req, workflowKey, expectedFirstRunID, explicit, action)
		if err != nil {
			if isStaleCurrentRunError(err) {
				lastErr = err
				continue
			}
			return resolveStep{}, err
		}
		return step, nil
	}
	return resolveStep{}, lastErr
}

func resolveOnce(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req resolveRequest,
	workflowKey definition.WorkflowKey,
	expectedFirstRunID string,
	explicit bool,
	action resolveActionFunc,
) (resolveStep, error) {
	var (
		step     resolveStep
		cleanErr error
	)
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			ms := lease.GetMutableState()
			actualKey := lease.GetContext().GetWorkflowKey()
			if ms.GetExecutionState().GetState() == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
				cleanErr = serviceerror.NewFailedPrecondition("target workflow execution is in zombie state")
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			firstRunID, err := ms.GetFirstRunID(ctx)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			if expectedFirstRunID != "" && (firstRunID == "" || firstRunID != expectedFirstRunID) {
				cleanErr = errDifferentChain
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			if ms.IsWorkflowExecutionRunning() {
				if isSelfWait(req.caller, actualKey) {
					cleanErr = newSelfWaitError()
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				if firstRunID == "" {
					cleanErr = serviceerror.NewInternal("workflow execution is missing first execution run ID")
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				step.target = resolvedTarget{
					workflowKey: actualKey,
					firstRunID:  firstRunID,
				}
				return runResolvedAction(lease, step.target, action)
			}

			closeEvent, nextRunID, superseded, err := readTerminalCloseEvent(ctx, ms, explicit)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			if superseded {
				if explicit {
					step.follow = true
					step.target.firstRunID = firstRunID
					step.nextRunID = nextRunID
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				cleanErr = consts.ErrLocateCurrentWorkflowExecution
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			if isSelfWait(req.caller, actualKey) {
				cleanErr = newSelfWaitError()
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			step.target = resolvedTarget{
				workflowKey: actualKey,
				firstRunID:  firstRunID,
				closeEvent:  closeEvent,
			}
			return runResolvedAction(lease, step.target, action)
		},
		nil,
		shard,
		checker,
	)
	if err != nil {
		return resolveStep{}, err
	}
	if cleanErr != nil {
		return resolveStep{}, cleanErr
	}
	return step, nil
}

// readTerminalCloseEvent returns a cloned terminal close event for a closed run,
// or superseded=true (with nextRunID when known) if the run has a chain successor.
// When followReset is false (the run is already confirmed current), reset markers
// are treated as final rather than superseded.
func readTerminalCloseEvent(
	ctx context.Context,
	ms historyi.MutableState,
	followReset bool,
) (*historypb.HistoryEvent, string, bool, error) {
	if nextRunID, ok := mutableStateSuccessor(ms, followReset); ok {
		return nil, nextRunID, true, nil
	}
	completionEvent, err := ms.GetCompletionEvent(ctx)
	if err != nil {
		return nil, "", false, err
	}
	if completionEvent == nil {
		return nil, "", false, serviceerror.NewInternal("missing workflow completion event")
	}
	if nextRunID, ok := completionEventSuccessor(completionEvent, followReset); ok {
		return nil, nextRunID, true, nil
	}
	return common.CloneProto(completionEvent), "", false, nil
}

func runResolvedAction(
	lease api.WorkflowLease,
	resolved resolvedTarget,
	action resolveActionFunc,
) (*api.UpdateWorkflowAction, error) {
	closed := resolved.closeEvent != nil
	if action == nil {
		return &api.UpdateWorkflowAction{Noop: true}, nil
	}
	postActions, err := action(lease, resolved)
	if err != nil {
		return nil, err
	}
	if closed && postActions != nil && !postActions.Noop {
		return nil, serviceerror.NewInternal("closed workflow execution cannot be mutated")
	}
	if closed || postActions == nil {
		return &api.UpdateWorkflowAction{Noop: true}, nil
	}
	return postActions, nil
}

func mutableStateSuccessor(ms historyi.MutableState, followReset bool) (string, bool) {
	info := ms.GetExecutionInfo()
	runID := ms.GetExecutionState().GetRunId()
	if followReset {
		if resetRunID := info.GetResetRunId(); resetRunID != "" && resetRunID != runID {
			return resetRunID, true
		}
	}
	if newRunID := info.GetNewExecutionRunId(); newRunID != "" && newRunID != runID {
		return newRunID, true
	}
	// shortcut: a run terminated by a reset of an earlier base has no forward link; after workflow ID reuse this returns FAILED_PRECONDITION. Upgrade by walking back to the base run's ResetRunId.
	if followReset && info.GetWorkflowWasReset() {
		return "", true
	}
	return "", false
}

func completionEventSuccessor(event *historypb.HistoryEvent, followReset bool) (string, bool) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId(), true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		if newRunID := event.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId(); newRunID != "" {
			return newRunID, true
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		if newRunID := event.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId(); newRunID != "" {
			return newRunID, true
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		if newRunID := event.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId(); newRunID != "" {
			return newRunID, true
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		if followReset && ndc.IsTerminatedByResetter(event) {
			return "", true
		}
	default:
	}
	return "", false
}

func isSelfWait(
	caller *historyservice.NexusCallerInfo,
	resolvedKey definition.WorkflowKey,
) bool {
	return caller.GetNamespaceId() == resolvedKey.NamespaceID &&
		caller.GetWorkflowId() == resolvedKey.WorkflowID &&
		caller.GetRunId() == resolvedKey.RunID
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
