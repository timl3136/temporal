package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/proto"
)

// GetWorkflowExecutionResultOperation is the name of the WorkflowService system Nexus operation.
const GetWorkflowExecutionResultOperation = "GetWorkflowExecutionResult"

// WorkflowResultFromCloseEvent converts a terminal workflow event into a detached Nexus outcome.
// The limit applies to the serialized Temporal payload or failure, including metadata and nested causes.
// A nil payload and no errors means the workflow completed without a result.
func WorkflowResultFromCloseEvent(event *historypb.HistoryEvent, limit int) (*commonpb.Payload, *nexus.OperationError, error) {
	var cause *failurepb.Failure
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attrs := event.GetWorkflowExecutionCompletedEventAttributes()
		if attrs == nil || attrs.GetNewExecutionRunId() != "" {
			break
		}
		values := attrs.GetResult().GetPayloads()
		switch len(values) {
		case 0:
			return nil, nil, nil
		case 1:
			if proto.Size(values[0]) > limit {
				cause = failure.NewServerFailure("workflow result exceeds the size limit", true)
				break
			}
			return common.CloneProto(values[0]), nil, nil
		default:
			cause = failure.NewServerFailure("workflow result contains multiple payloads", true)
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := event.GetWorkflowExecutionFailedEventAttributes()
		if attrs.GetNewExecutionRunId() == "" {
			cause = attrs.GetFailure()
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		attrs := event.GetWorkflowExecutionCanceledEventAttributes()
		if attrs != nil {
			cause = &failurepb.Failure{
				Message: "workflow canceled",
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{Details: attrs.GetDetails()},
				},
			}
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		if attrs := event.GetWorkflowExecutionTerminatedEventAttributes(); attrs != nil {
			cause = terminatedWorkflowResultFailure(attrs)
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		if attrs := event.GetWorkflowExecutionTimedOutEventAttributes(); attrs != nil && attrs.GetNewExecutionRunId() == "" {
			cause = failure.NewTimeoutFailure("workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
		}
	default:
		return nil, nil, serviceerror.NewInternalf("expected a workflow close event, got %v", event.GetEventType())
	}
	if cause == nil {
		return nil, nil, serviceerror.NewInternalf("incomplete or unresolved workflow close event %v", event.GetEventType())
	}
	if proto.Size(cause) > limit {
		cause = failure.NewServerFailure("workflow result exceeds the size limit", true)
		if proto.Size(cause) > limit {
			// Keep the non-retryable flag even if the limit cannot fit the minimal failure.
			cause.Message = ""
		}
	}

	nexusFailure, err := TemporalFailureToNexusFailure(cause)
	if err != nil {
		return nil, nil, err
	}
	state := nexus.OperationStateFailed
	// History's wire response uses the top-level failure type to convey cancellation.
	if cause.GetCanceledFailureInfo() != nil {
		state = nexus.OperationStateCanceled
	}
	opErr := &nexus.OperationError{State: state, Cause: &nexus.FailureError{Failure: nexusFailure}}
	if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
		return nil, nil, err
	}
	return nil, opErr, nil
}

func terminatedWorkflowResultFailure(attrs *historypb.WorkflowExecutionTerminatedEventAttributes) *failurepb.Failure {
	cause := &failurepb.Failure{
		Message: attrs.GetReason(),
		FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
			TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{Identity: attrs.GetIdentity()},
		},
	}
	// TerminatedFailureInfo has no details field. Preserve the original payloads in the cause.
	if details := attrs.GetDetails(); details != nil {
		cause.Cause = &failurepb.Failure{
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Details: details},
			},
		}
	}
	return cause
}
