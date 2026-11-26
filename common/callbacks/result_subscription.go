package callbacks

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	commonnexus "go.temporal.io/server/common/nexus"
)

// ResultSubscriptionFormatVersionV1 is the format version for GetWorkflowExecutionResult callbacks.
const ResultSubscriptionFormatVersionV1 int32 = 1

// ErrResultSubscriptionNonTerminalClose indicates a workflow close event points to a successor run
// (such as a cron, retry, or continue-as-new run) and cannot produce a terminal result outcome.
var ErrResultSubscriptionNonTerminalClose = errors.New("workflow close event has a successor run")

// ValidateResultSubscriptionForDelivery checks that a persisted result subscription callback uses a
// supported format version, carries a subscription request ID, and targets an internal or system URL.
func ValidateResultSubscriptionForDelivery(
	url string,
	sub *commonpb.Callback_ResultSubscription,
) error {
	if sub.GetResultFormatVersion() != ResultSubscriptionFormatVersionV1 {
		return fmt.Errorf("unsupported result subscription format version: %d", sub.GetResultFormatVersion())
	}
	if sub.GetRequestId() == "" {
		return errors.New("result subscription callback missing request ID")
	}
	if url != commonnexus.SystemCallbackURL && url != chasm.NexusCompletionHandlerURL {
		return fmt.Errorf("unsupported callback URL for result subscription: %q", url)
	}
	return nil
}
