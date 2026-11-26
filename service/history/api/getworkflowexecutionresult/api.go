package getworkflowexecutionresult

import (
	"cmp"
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	hsmcallbacks "go.temporal.io/server/service/history/hsm/callbacks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	resultSubscriptionTokenVersion int32 = 1
	resultFormatVersionV1          int32 = 1
)

type StartRequest struct {
	NamespaceID namespace.ID                    // trusted, from the invocation envelope
	Caller      *historyservice.NexusCallerInfo // S's field; nil is rejected (workflow-only)
	Target      *commonpb.WorkflowExecution     // the request's only field
	RequestID   string                          // caller's scheduled request ID; dedup key
	Callback    *commonpb.Callback              // validated by the Nexus handler
}

// StartResult holds the outcome of Start; exactly one field is set.
type StartResult struct {
	Closed     *historypb.HistoryEvent // cloned terminal close event; F1 converts it with B
	Subscribed *Subscription
}

type Subscription struct {
	Token string         // encoded ResultSubscriptionToken
	Link  *commonpb.Link // to the resolved run
}

// Start resolves req.Target under its workflow lease and either returns a cloned terminal close
// event or atomically attaches a result subscription callback.
//
// Rejections are serviceerror values: InvalidArgument, NotFound, FailedPrecondition,
// ResourceExhausted (cap or history budget). Anything else is retryable infrastructure.
func Start(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	req StartRequest,
) (StartResult, error) {
	if err := validateStartRequest(req); err != nil {
		return StartResult{}, err
	}
	nsEntry, err := api.GetActiveNamespace(shard, req.NamespaceID, req.Target.GetWorkflowId())
	if err != nil {
		return StartResult{}, err
	}
	nsName := nsEntry.Name().String()

	var (
		result    StartResult
		rejectErr error
	)
	_, err = resolveTarget(
		ctx,
		shard,
		checker,
		resolveRequest{
			namespaceID: req.NamespaceID,
			caller:      req.Caller,
			target:      req.Target,
		},
		func(lease api.WorkflowLease, target resolvedTarget) (*api.UpdateWorkflowAction, error) {
			// Reset captured outputs in case resolveTarget retries after a stale current run.
			result = StartResult{}
			rejectErr = nil

			res, action, rErr, err := startOnLease(ctx, shard, nsName, req, lease, target)
			if err != nil {
				return nil, err
			}
			result = res
			rejectErr = rErr
			return action, nil
		},
	)
	if err != nil {
		return StartResult{}, err
	}
	if rejectErr != nil {
		return StartResult{}, rejectErr
	}
	return result, nil
}

func startOnLease(
	ctx context.Context,
	shard historyi.ShardContext,
	nsName string,
	req StartRequest,
	lease api.WorkflowLease,
	target resolvedTarget,
) (result StartResult, action *api.UpdateWorkflowAction, rejectErr error, err error) {
	ms := lease.GetMutableState()

	// Deduplicate before checking feature flag, closed status, or capacity so retries of
	// accepted waits (including inherited callbacks and lost responses after close) return
	// their original resolved-run token without writing.
	existing, found, err := findResultSubscription(ctx, ms, req.RequestID)
	if err != nil {
		return StartResult{}, nil, nil, err
	}
	if found {
		if err := verifyStoredCallbackCaller(existing.url, existing.header, req.Caller); err != nil {
			return StartResult{}, &api.UpdateWorkflowAction{Noop: true}, err, nil
		}
		resolvedRunID := cmp.Or(existing.sub.GetResolvedExecution().GetRunId(), target.workflowKey.RunID)
		sub, err := buildSubscription(nsName, req.NamespaceID.String(), target.workflowKey.WorkflowID, target.firstRunID, resolvedRunID, req.RequestID)
		if err != nil {
			return StartResult{}, &api.UpdateWorkflowAction{Noop: true}, err, nil
		}
		return StartResult{Subscribed: sub}, &api.UpdateWorkflowAction{Noop: true}, nil, nil
	}

	if !shard.GetConfig().EnableGetWorkflowExecutionResultFromWorkflow(nsName) {
		return StartResult{}, &api.UpdateWorkflowAction{Noop: true}, serviceerror.NewFailedPrecondition("GetWorkflowExecutionResult operation is disabled"), nil
	}

	if target.closeEvent != nil {
		return StartResult{Closed: target.closeEvent}, &api.UpdateWorkflowAction{Noop: true}, nil, nil
	}

	return attachResultSubscriptionOnLease(ctx, shard, nsName, req, lease, target)
}

func attachResultSubscriptionOnLease(
	ctx context.Context,
	shard historyi.ShardContext,
	nsName string,
	req StartRequest,
	lease api.WorkflowLease,
	target resolvedTarget,
) (result StartResult, action *api.UpdateWorkflowAction, rejectErr error, err error) {
	ms := lease.GetMutableState()
	cb := common.CloneProto(req.Callback)
	cb.ResultSubscription = &commonpb.Callback_ResultSubscription{
		RequestId:           req.RequestID,
		ResultFormatVersion: resultFormatVersionV1,
		ResolvedExecution: &commonpb.WorkflowExecution{
			WorkflowId: target.workflowKey.WorkflowID,
			RunId:      target.workflowKey.RunID,
		},
	}

	collision, err := hasConflictingNonResultCallback(ctx, ms, req.RequestID)
	if err != nil {
		return StartResult{}, nil, nil, err
	}
	if collision {
		return StartResult{}, &api.UpdateWorkflowAction{Noop: true}, serviceerror.NewInvalidArgument("request ID is already attached to a different callback"), nil
	}

	if err := checkTargetHistoryBudget(shard.GetConfig(), nsName, ms, req.RequestID, cb); err != nil {
		return StartResult{}, &api.UpdateWorkflowAction{Noop: true}, err, nil
	}

	if err := validateCallbackCapacity(ctx, shard.GetConfig(), nsName, lease, req.RequestID, cb); err != nil {
		if _, isResExhausted := errors.AsType[*serviceerror.ResourceExhausted](err); isResExhausted {
			return StartResult{}, &api.UpdateWorkflowAction{Noop: true}, err, nil
		}
		return StartResult{}, nil, nil, err
	}

	sub, err := buildSubscription(nsName, req.NamespaceID.String(), target.workflowKey.WorkflowID, target.firstRunID, target.workflowKey.RunID, req.RequestID)
	if err != nil {
		return StartResult{}, &api.UpdateWorkflowAction{Noop: true}, err, nil
	}

	_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(
		nil,   // versioningOverride
		false, // unsetVersioningOverride
		req.RequestID,
		[]*commonpb.Callback{cb},
		nil,   // links
		"",    // identity
		nil,   // priority
		nil,   // timeSkippingConfig
		false, // timeSkippingConfigUpdated
		nil,   // workflowUpdateOptions
	)
	if err != nil {
		return StartResult{}, nil, nil, err
	}

	return StartResult{Subscribed: sub}, api.UpdateWorkflowWithoutWorkflowTask, nil, nil
}

// Cancel explicitly cancels one result subscription identified by token on the target chain.
// Returns nil for an already-settled, already-canceled or delivered subscription.
func Cancel(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	namespaceID namespace.ID,
	caller *historyservice.NexusCallerInfo,
	token string,
) error {
	tok, err := validateCancelInputs(namespaceID, caller, token)
	if err != nil {
		return err
	}
	if _, err := api.GetActiveNamespace(shard, namespaceID, tok.GetWorkflowId()); err != nil {
		return err
	}

	currentKey := definition.NewWorkflowKey(namespaceID.String(), tok.GetWorkflowId(), "")
	_, err = cancelOnRunWithRetry(ctx, shard, checker, currentKey, tok, caller, false)
	if err == nil {
		return nil
	}
	_, isNotFound := errors.AsType[*serviceerror.NotFound](err)
	if !errors.Is(err, errDifferentChain) && !isNotFound {
		return err
	}

	// Current execution belongs to a different chain (or is absent). Walk forward from the
	// resolved run without modifying any unrelated execution.
	return cancelByWalkingChain(ctx, shard, checker, namespaceID, tok, caller)
}

func validateStartRequest(req StartRequest) error {
	err := validateResolveRequest(resolveRequest{
		namespaceID: req.NamespaceID,
		caller:      req.Caller,
		target:      req.Target,
	})
	if err != nil {
		return err
	}
	if req.RequestID == "" {
		return serviceerror.NewInvalidArgument("request ID is required")
	}
	if req.Caller.GetRequestId() != "" && req.Caller.GetRequestId() != req.RequestID {
		return serviceerror.NewInvalidArgument("caller request ID does not match request ID")
	}
	if req.Callback == nil {
		return serviceerror.NewInvalidArgument("callback is required")
	}
	if req.Callback.GetResultSubscription() != nil {
		return serviceerror.NewInvalidArgument("callback must not specify result_subscription")
	}
	nexusCB := req.Callback.GetNexus()
	if nexusCB == nil || nexusCB.GetUrl() == "" {
		return serviceerror.NewInvalidArgument("nexus callback URL is required")
	}
	nsID, wfID, runID, tokReqID, err := extractCallbackCaller(nexusCB.GetUrl(), nexusCB.GetHeader())
	if err != nil {
		return err
	}
	if nsID != req.Caller.GetNamespaceId() || wfID != req.Caller.GetWorkflowId() || runID != req.Caller.GetRunId() {
		return serviceerror.NewInvalidArgument("callback caller identity does not match caller info")
	}
	if tokReqID != "" && tokReqID != req.RequestID {
		return serviceerror.NewInvalidArgument("callback request ID does not match request ID")
	}
	return nil
}

func validateCancelInputs(
	namespaceID namespace.ID,
	caller *historyservice.NexusCallerInfo,
	token string,
) (*tokenspb.ResultSubscriptionToken, error) {
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}
	tok, err := decodeResultSubscriptionToken(token)
	if err != nil {
		return nil, err
	}
	if tok.GetNamespaceId() != namespaceID.String() {
		return nil, serviceerror.NewInvalidArgument("subscription token namespace does not match request namespace")
	}
	if caller == nil || caller.GetNamespaceId() == "" || caller.GetWorkflowId() == "" || caller.GetRunId() == "" {
		return nil, serviceerror.NewInvalidArgument("caller workflow identity is required")
	}
	if caller.GetNamespaceId() != namespaceID.String() {
		return nil, serviceerror.NewInvalidArgument("caller namespace does not match target namespace")
	}
	if caller.GetRequestId() != "" && caller.GetRequestId() != tok.GetRequestId() {
		return nil, serviceerror.NewInvalidArgument("caller request ID does not match subscription token")
	}
	return tok, nil
}

type cancelStep struct {
	follow    bool
	nextRunID string
}

func cancelByWalkingChain(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	namespaceID namespace.ID,
	tok *tokenspb.ResultSubscriptionToken,
	caller *historyservice.NexusCallerInfo,
) error {
	visited := make(map[string]struct{})
	runID := tok.GetResolvedRunId()
	for range maxChainWalkHops + 1 {
		if runID == "" {
			return serviceerror.NewFailedPrecondition("target workflow execution chain is broken: missing successor run")
		}
		if _, seen := visited[runID]; seen {
			return serviceerror.NewFailedPrecondition("target workflow execution chain contains a cycle")
		}
		visited[runID] = struct{}{}

		hopKey := definition.NewWorkflowKey(namespaceID.String(), tok.GetWorkflowId(), runID)
		step, err := cancelOnRunWithRetry(ctx, shard, checker, hopKey, tok, caller, true)
		if err != nil {
			if _, isNotFound := errors.AsType[*serviceerror.NotFound](err); isNotFound {
				// Target run or its successor was deleted by retention; subscription is already settled.
				return nil
			}
			return err
		}
		if !step.follow {
			return nil
		}
		runID = step.nextRunID
	}
	return serviceerror.NewFailedPrecondition("target workflow execution chain exceeded maximum walk hops")
}

func cancelOnRunWithRetry(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	workflowKey definition.WorkflowKey,
	tok *tokenspb.ResultSubscriptionToken,
	caller *historyservice.NexusCallerInfo,
	explicit bool,
) (cancelStep, error) {
	var lastErr error
	for range maxStaleCurrentRunAttempts {
		if err := ctx.Err(); err != nil {
			return cancelStep{}, err
		}
		step, err := cancelOnRunOnce(ctx, shard, checker, workflowKey, tok, caller, explicit)
		if err != nil {
			if isStaleCurrentRunError(err) || persistence.IsConflictErr(err) {
				lastErr = err
				continue
			}
			return cancelStep{}, err
		}
		return step, nil
	}
	return cancelStep{}, lastErr
}

func cancelOnRunOnce(
	ctx context.Context,
	shard historyi.ShardContext,
	checker api.WorkflowConsistencyChecker,
	workflowKey definition.WorkflowKey,
	tok *tokenspb.ResultSubscriptionToken,
	caller *historyservice.NexusCallerInfo,
	explicit bool,
) (cancelStep, error) {
	var (
		step     cancelStep
		cleanErr error
	)
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			step = cancelStep{}
			cleanErr = nil

			ms := lease.GetMutableState()
			firstRunID, err := ms.GetFirstRunID(ctx)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			if firstRunID == "" || firstRunID != tok.GetFirstRunId() {
				if explicit {
					cleanErr = serviceerror.NewInvalidArgument("subscription token chain mismatch")
				} else {
					cleanErr = errDifferentChain
				}
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			if ms.GetExecutionState().GetState() == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
				cleanErr = serviceerror.NewFailedPrecondition("target workflow execution is in zombie state")
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			if ms.IsWorkflowExecutionRunning() {
				action, err, cErr := cancelSubscriptionOnLease(ctx, lease, tok, caller, true)
				cleanErr = cErr
				return action, err
			}

			nextRunID, superseded, err := readChainSuccessor(ctx, ms, explicit)
			if err != nil {
				cleanErr = err
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}
			if superseded {
				if explicit {
					step.follow = true
					step.nextRunID = nextRunID
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
				cleanErr = consts.ErrLocateCurrentWorkflowExecution
				return &api.UpdateWorkflowAction{Noop: true}, nil
			}

			action, err, cErr := cancelSubscriptionOnLease(ctx, lease, tok, caller, false)
			cleanErr = cErr
			return action, err
		},
		nil,
		shard,
		checker,
	)
	if err != nil {
		return cancelStep{}, err
	}
	if cleanErr != nil {
		return cancelStep{}, cleanErr
	}
	return step, nil
}

func cancelSubscriptionOnLease(
	ctx context.Context,
	lease api.WorkflowLease,
	tok *tokenspb.ResultSubscriptionToken,
	caller *historyservice.NexusCallerInfo,
	running bool,
) (action *api.UpdateWorkflowAction, err error, cleanErr error) {
	ms := lease.GetMutableState()
	stored, found, err := findResultSubscription(ctx, ms, tok.GetRequestId())
	if err != nil {
		return nil, err, nil
	}
	if !found {
		return &api.UpdateWorkflowAction{Noop: true}, nil, nil
	}

	resolvedExec := stored.sub.GetResolvedExecution()
	if resolvedExec.GetWorkflowId() != tok.GetWorkflowId() || resolvedExec.GetRunId() != tok.GetResolvedRunId() {
		return &api.UpdateWorkflowAction{Noop: true}, nil, serviceerror.NewInvalidArgument("subscription token does not match stored subscription")
	}
	if err := verifyStoredCallbackCaller(stored.url, stored.header, caller); err != nil {
		return &api.UpdateWorkflowAction{Noop: true}, nil, err
	}
	if !running || !stored.cancelable {
		return &api.UpdateWorkflowAction{Noop: true}, nil, nil
	}

	_, err = ms.AddWorkflowExecutionOptionsUpdatedEventWithAttributes(
		&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
			CanceledResultSubscriptionRequestIds: []string{tok.GetRequestId()},
		},
		nil,
	)
	if err != nil {
		return nil, err, nil
	}
	return api.UpdateWorkflowWithoutWorkflowTask, nil, nil
}

func readChainSuccessor(
	ctx context.Context,
	ms historyi.MutableState,
	followReset bool,
) (string, bool, error) {
	if nextRunID, ok := mutableStateSuccessor(ms, followReset); ok {
		return nextRunID, true, nil
	}
	completionEvent, err := ms.GetCompletionEvent(ctx)
	if err != nil {
		return "", false, err
	}
	if completionEvent == nil {
		return "", false, serviceerror.NewInternal("missing workflow completion event")
	}
	if nextRunID, ok := completionEventSuccessor(completionEvent, followReset); ok {
		return nextRunID, true, nil
	}
	return "", false, nil
}

type storedSubscription struct {
	isChasm    bool
	sub        *commonpb.Callback_ResultSubscription
	url        string
	header     map[string]string
	cancelable bool
}

func findResultSubscription(
	ctx context.Context,
	ms historyi.MutableState,
	requestID string,
) (storedSubscription, bool, error) {
	if requestID == "" {
		return storedSubscription{}, false, nil
	}
	if ms.ChasmEnabled() {
		wf, readCtx, err := ms.ChasmWorkflowComponentReadOnly(ctx)
		if err != nil {
			return storedSubscription{}, false, err
		}
		if _, cb, ok := wf.FindResultSubscription(readCtx, requestID); ok {
			return storedSubscription{
				isChasm:    true,
				sub:        cb.GetCallback().GetResultSubscription(),
				url:        cb.GetCallback().GetNexus().GetUrl(),
				header:     cb.GetCallback().GetNexus().GetHeader(),
				cancelable: chasmcallback.TransitionCanceled.Possible(cb),
			}, true, nil
		}
	}
	if ms.HSM() != nil {
		coll := hsmcallbacks.MachineCollection(ms.HSM())
		for _, node := range coll.List() {
			cb, err := coll.Data(node.Key.ID)
			if err != nil {
				return storedSubscription{}, false, err
			}
			sub := cb.GetCallback().GetResultSubscription()
			if sub != nil && sub.GetRequestId() == requestID {
				return storedSubscription{
					isChasm:    false,
					sub:        sub,
					url:        cb.GetCallback().GetNexus().GetUrl(),
					header:     cb.GetCallback().GetNexus().GetHeader(),
					cancelable: hsmcallbacks.TransitionCanceled.Possible(cb),
				}, true, nil
			}
		}
	}
	return storedSubscription{}, false, nil
}

func hasConflictingNonResultCallback(
	ctx context.Context,
	ms historyi.MutableState,
	requestID string,
) (bool, error) {
	if ms.ChasmEnabled() {
		wf, _, err := ms.ChasmWorkflowComponentReadOnly(ctx)
		if err != nil {
			return false, err
		}
		if chasmcallback.HasCallbacksForRequest(wf, requestID) {
			return true, nil
		}
	}
	if ms.HSM() != nil {
		coll := hsmcallbacks.MachineCollection(ms.HSM())
		if _, err := coll.Data(fmt.Sprintf("cb-%s-0", requestID)); err == nil {
			return true, nil
		}
	}
	return false, nil
}

func checkTargetHistoryBudget(
	cfg *configs.Config,
	nsName string,
	ms historyi.MutableState,
	requestID string,
	cb *commonpb.Callback,
) error {
	if countLimit := cfg.HistoryCountLimitError(nsName); countLimit > 0 &&
		ms.GetNextEventID()+int64(ms.NumBufferedEvents()) > int64(countLimit) {
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "target workflow execution history event count limit exceeded",
		}
	}

	previewEvent := &historypb.HistoryEvent{
		EventId:         ms.GetNextEventID(),
		EventTime:       timestamppb.New(ms.Now()),
		Version:         ms.GetCurrentVersion(),
		EventType:       enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		WorkerMayIgnore: true,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
			WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				AttachedRequestId:           requestID,
				AttachedCompletionCallbacks: []*commonpb.Callback{cb},
			},
		},
	}
	estimatedEventSize := int64(proto.Size(previewEvent))
	currentHistorySize := ms.GetExecutionInfo().GetExecutionStats().GetHistorySize()
	if sizeLimit := cfg.HistorySizeLimitError(nsName); sizeLimit > 0 && currentHistorySize+estimatedEventSize > int64(sizeLimit) {
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "target workflow execution history size limit exceeded",
		}
	}

	estimatedMSSize := int64(cb.Size() + len(requestID))
	if msSizeLimit := cfg.MutableStateSizeLimitError(); msSizeLimit > 0 &&
		int64(ms.GetApproximatePersistedSize())+estimatedMSSize > int64(msSizeLimit) {
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "target workflow execution mutable state size limit exceeded",
		}
	}
	return nil
}

func validateCallbackCapacity(
	ctx context.Context,
	cfg *configs.Config,
	nsName string,
	lease api.WorkflowLease,
	requestID string,
	cb *commonpb.Callback,
) error {
	ms := lease.GetMutableState()
	if ms.ChasmEnabled() && cfg.EnableCHASMCallbacks(nsName) {
		inFlight, err := lease.GetContext().UpdateRegistry(ctx).InFlightCallbacks()
		if err != nil {
			return err
		}
		err = ms.ValidateCallbackAddition(inFlight, chasmworkflow.CallbackAddition{
			RequestID: requestID,
			Callbacks: []*commonpb.Callback{cb},
		})
		if err != nil {
			if fpErr, ok := errors.AsType[*serviceerror.FailedPrecondition](err); ok {
				return &serviceerror.ResourceExhausted{
					Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
					Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
					Message: fpErr.Error(),
				}
			}
			return err
		}
		return nil
	}

	if ms.HSM() != nil {
		coll := hsmcallbacks.MachineCollection(ms.HSM())
		maxCallbacks := cfg.MaxCallbacksPerWorkflow(nsName)
		if coll.Size()+1 > maxCallbacks {
			return &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
				Message: fmt.Sprintf(
					"cannot attach more than %d callbacks to a workflow (%d callbacks already attached)",
					maxCallbacks,
					coll.Size(),
				),
			}
		}
	}
	return nil
}

func buildSubscription(
	nsName string,
	nsID string,
	workflowID string,
	firstRunID string,
	resolvedRunID string,
	requestID string,
) (*Subscription, error) {
	encodedToken, err := encodeResultSubscriptionToken(&tokenspb.ResultSubscriptionToken{
		Version:       resultSubscriptionTokenVersion,
		NamespaceId:   nsID,
		WorkflowId:    workflowID,
		FirstRunId:    firstRunID,
		ResolvedRunId: resolvedRunID,
		RequestId:     requestID,
	})
	if err != nil {
		return nil, err
	}
	return &Subscription{
		Token: encodedToken,
		Link: api.GenerateRequestIDRefLink(
			nsName,
			workflowID,
			resolvedRunID,
			requestID,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		),
	}, nil
}

func encodeResultSubscriptionToken(tok *tokenspb.ResultSubscriptionToken) (string, error) {
	if err := validateResultSubscriptionToken(tok); err != nil {
		return "", err
	}
	raw, err := proto.Marshal(tok)
	if err != nil {
		return "", serviceerror.NewInternalf("failed to marshal result subscription token: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeResultSubscriptionToken(encoded string) (*tokenspb.ResultSubscriptionToken, error) {
	if encoded == "" {
		return nil, serviceerror.NewInvalidArgument("result subscription token is required")
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid result subscription token")
	}
	tok := &tokenspb.ResultSubscriptionToken{}
	if err := proto.Unmarshal(raw, tok); err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid result subscription token")
	}
	if err := validateResultSubscriptionToken(tok); err != nil {
		return nil, err
	}
	return tok, nil
}

func validateResultSubscriptionToken(tok *tokenspb.ResultSubscriptionToken) error {
	if tok == nil {
		return serviceerror.NewInvalidArgument("invalid result subscription token")
	}
	if tok.GetVersion() != resultSubscriptionTokenVersion {
		return serviceerror.NewInvalidArgumentf("unsupported result subscription token version: %d", tok.GetVersion())
	}
	if tok.GetNamespaceId() == "" || uuid.Validate(tok.GetNamespaceId()) != nil {
		return serviceerror.NewInvalidArgument("invalid namespace ID in result subscription token")
	}
	if tok.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("missing workflow ID in result subscription token")
	}
	if tok.GetFirstRunId() == "" || uuid.Validate(tok.GetFirstRunId()) != nil {
		return serviceerror.NewInvalidArgument("invalid first run ID in result subscription token")
	}
	if tok.GetResolvedRunId() == "" || uuid.Validate(tok.GetResolvedRunId()) != nil {
		return serviceerror.NewInvalidArgument("invalid resolved run ID in result subscription token")
	}
	if tok.GetRequestId() == "" {
		return serviceerror.NewInvalidArgument("missing request ID in result subscription token")
	}
	return nil
}

func verifyStoredCallbackCaller(
	url string,
	header map[string]string,
	caller *historyservice.NexusCallerInfo,
) error {
	nsID, wfID, runID, _, err := extractCallbackCaller(url, header)
	if err != nil {
		return serviceerror.NewInvalidArgument("stored callback does not carry valid caller identity")
	}
	if nsID != caller.GetNamespaceId() || wfID != caller.GetWorkflowId() || runID != caller.GetRunId() {
		return serviceerror.NewInvalidArgument("caller identity does not match stored subscription")
	}
	return nil
}

func extractCallbackCaller(
	url string,
	header map[string]string,
) (namespaceID, workflowID, runID, requestID string, err error) {
	encodedToken := nexus.Header(header).Get(commonnexus.CallbackTokenHeader)
	if encodedToken == "" {
		return "", "", "", "", serviceerror.NewInvalidArgument("callback missing token header")
	}
	if url == chasm.NexusCompletionHandlerURL {
		refBytes, reqID, err := chasm.UnpackNexusCallbackToken(encodedToken)
		if err != nil {
			return "", "", "", "", serviceerror.NewInvalidArgumentf("invalid internal callback token: %v", err)
		}
		ref := &persistencespb.ChasmComponentRef{}
		if err := ref.Unmarshal(refBytes); err != nil {
			return "", "", "", "", serviceerror.NewInvalidArgumentf("invalid internal callback component reference: %v", err)
		}
		if ref.GetNamespaceId() == "" || ref.GetBusinessId() == "" || ref.GetRunId() == "" {
			return "", "", "", "", serviceerror.NewInvalidArgument("internal callback component reference missing caller identity")
		}
		return ref.GetNamespaceId(), ref.GetBusinessId(), ref.GetRunId(), reqID, nil
	}

	cbToken, err := commonnexus.DecodeCallbackToken(encodedToken)
	if err != nil {
		return "", "", "", "", serviceerror.NewInvalidArgumentf("invalid callback token: %v", err)
	}
	completion, err := commonnexus.NewCallbackTokenGenerator().DecodeCompletion(cbToken)
	if err != nil {
		return "", "", "", "", serviceerror.NewInvalidArgumentf("invalid callback completion token: %v", err)
	}
	nsID, wfID, rID, err := commonnexus.CompletionTarget(completion)
	if err != nil {
		return "", "", "", "", serviceerror.NewInvalidArgumentf("invalid callback completion target: %v", err)
	}
	if nsID == "" || wfID == "" || rID == "" {
		return "", "", "", "", serviceerror.NewInvalidArgument("callback token missing caller identity")
	}
	return nsID, wfID, rID, completion.GetRequestId(), nil
}
