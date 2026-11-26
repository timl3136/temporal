package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// CompletionCallbacks implements callback.Holder.
func (w *Workflow) CompletionCallbacks() chasm.Map[string, *callback.Callback] {
	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback])
	}
	return w.Callbacks
}

// CompletionCallbackMetadata implements callback.Host. The Workflow holds the
// metadata for every callback in the execution, including those attached to its WorkflowUpdates.
func (w *Workflow) CompletionCallbackMetadata() *callbackspb.CallbackMetadata {
	if w.CallbackMetadata == nil {
		w.CallbackMetadata = &callbackspb.CallbackMetadata{}
	}
	return w.CallbackMetadata
}

// GetNexusCompletion implements callback.CompletionSource.
func (w *Workflow) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	// Retrieve the completion data from the underlying mutable state via MSPointer.
	return w.MSPointer.GetNexusCompletion(ctx, requestID)
}

// FindResultSubscription returns the callback whose ResultSubscription matches requestID.
func (w *Workflow) FindResultSubscription(
	ctx chasm.Context,
	requestID string,
) (*callback.Callback, bool) {
	if requestID == "" {
		return nil, false
	}
	for _, field := range w.Callbacks {
		cb, ok := field.TryGet(ctx)
		if !ok {
			continue
		}
		if cb.GetCallback().GetResultSubscription().GetRequestId() == requestID {
			return cb, true
		}
	}
	return nil, false
}

// CancelableResultSubscriptionKeys returns the CHASM map keys of callbacks whose
// ResultSubscription.RequestId is in requestIDs and whose status can transition to canceled.
func (w *Workflow) CancelableResultSubscriptionKeys(
	ctx chasm.Context,
	requestIDs map[string]struct{},
) []string {
	if len(requestIDs) == 0 || len(w.Callbacks) == 0 {
		return nil
	}
	var keys []string
	for key, field := range w.Callbacks {
		cb, ok := field.TryGet(ctx)
		if !ok {
			continue
		}
		subReqID := cb.GetCallback().GetResultSubscription().GetRequestId()
		if _, ok := requestIDs[subReqID]; !ok {
			continue
		}
		if callback.TransitionCanceled.Possible(cb) {
			keys = append(keys, key)
		}
	}
	return keys
}

// CancelResultSubscriptions transitions the callbacks at keys to CALLBACK_STATUS_FAILED
// with a canceled failure.
func (w *Workflow) CancelResultSubscriptions(
	ctx chasm.MutableContext,
	keys []string,
) error {
	for _, key := range keys {
		field, ok := w.Callbacks[key]
		if !ok {
			continue
		}
		cb := field.Get(ctx)
		if !callback.TransitionCanceled.Possible(cb) {
			continue
		}
		if err := callback.TransitionCanceled.Apply(cb, ctx, callback.EventCanceled{}); err != nil {
			return err
		}
	}
	return nil
}
