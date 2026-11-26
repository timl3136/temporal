package workflow

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
)

type getWorkflowExecutionResultProcessor struct {
	config Config
}

// NewGetWorkflowExecutionResultOperationProcessor returns the caller-side processor for the GetWorkflowExecutionResult
// system Nexus operation.
func NewGetWorkflowExecutionResultOperationProcessor(config Config) chasm.RegisterableNexusOperationProcessor {
	return chasm.NewRegisterableNexusOperationProcessor(getWorkflowExecutionResultProcessor{config: config})
}

// ProcessInput admits the operation at schedule time only when the namespace enables it. On every other call it only
// validates and routes, so an operation accepted before the flag was disabled can still be started or canceled.
// Invalid input is returned as a failed operation error: the schedule command is accepted and the operation fails
// with it when invoked, before any history shard is contacted.
func (o getWorkflowExecutionResultProcessor) ProcessInput(
	ctx chasm.NexusOperationProcessorContext,
	request *workflowservice.GetWorkflowExecutionResultRequest,
) (*chasm.NexusOperationProcessorResult, error) {
	if ctx.Scheduling && !o.config.enableGetWorkflowExecutionResultFromWorkflow(ctx.Namespace.Name().String()) {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "GetWorkflowExecutionResult operation is disabled")
	}
	caller := ctx.Caller
	if caller.GetWorkflowId() == "" {
		return nil, invalidResultRequest("GetWorkflowExecutionResult can only be called from a workflow")
	}
	execution := request.GetExecution()
	if execution.GetWorkflowId() == "" {
		return nil, invalidResultRequest("WorkflowId is not set on the execution")
	}
	if execution.GetRunId() != "" && uuid.Validate(execution.GetRunId()) != nil {
		return nil, invalidResultRequest("invalid RunId %q", execution.GetRunId())
	}
	// A running caller is the current run of its workflow ID, so an empty run ID also names itself.
	if execution.GetWorkflowId() == caller.GetWorkflowId() &&
		(execution.GetRunId() == "" || execution.GetRunId() == caller.GetRunId()) {
		return nil, invalidResultRequest("a workflow cannot wait for its own result")
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  execution.GetWorkflowId(),
		},
	}, nil
}

func invalidResultRequest(format string, args ...any) error {
	message := "InvalidArgument: " + fmt.Sprintf(format, args...)
	return &nexus.OperationError{
		State:   nexus.OperationStateFailed,
		Message: message,
		// HSM records an operation error from its original failure.
		OriginalFailure: &nexus.Failure{Message: message},
	}
}
