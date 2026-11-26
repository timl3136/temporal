package workflow

import (
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/tests"
)

const (
	callerWorkflowID = "caller-wf"
	callerRunID      = "11111111-1111-1111-1111-111111111111"
	targetRunID      = "22222222-2222-2222-2222-222222222222"
)

func newResultProcessor(enabled bool) getWorkflowExecutionResultProcessor {
	return getWorkflowExecutionResultProcessor{config: Config{
		enableGetWorkflowExecutionResultFromWorkflow: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(enabled),
	}}
}

func resultProcessorContext(scheduling bool) chasm.NexusOperationProcessorContext {
	return chasm.NexusOperationProcessorContext{
		Namespace:  tests.GlobalNamespaceEntry,
		RequestID:  "request-id",
		Scheduling: scheduling,
		Caller: &historyservice.NexusCallerInfo{
			NamespaceId: tests.GlobalNamespaceEntry.ID().String(),
			WorkflowId:  callerWorkflowID,
			RunId:       callerRunID,
			RequestId:   "request-id",
		},
	}
}

func resultRequest(workflowID, runID string) *workflowservice.GetWorkflowExecutionResultRequest {
	return &workflowservice.GetWorkflowExecutionResultRequest{
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	}
}

func TestGetWorkflowExecutionResultProcessor_Admission(t *testing.T) {
	t.Run("scheduling with flag off is rejected as a bad request", func(t *testing.T) {
		_, err := newResultProcessor(false).ProcessInput(resultProcessorContext(true), resultRequest("target", ""))
		handlerErr, ok := errors.AsType[*nexus.HandlerError](err)
		require.True(t, ok, "expected handler error, got %v", err)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	})

	// Start and Cancel re-run the processor; disabling the flag must not strand accepted operations.
	t.Run("routing with flag off still routes", func(t *testing.T) {
		res, err := newResultProcessor(false).ProcessInput(resultProcessorContext(false), resultRequest("target", ""))
		require.NoError(t, err)
		require.Equal(t, chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: tests.GlobalNamespaceEntry.ID().String(),
			BusinessID:  "target",
		}, res.RoutingKey)
	})
}

func TestGetWorkflowExecutionResultProcessor_Routing(t *testing.T) {
	for _, tc := range []struct {
		name       string
		workflowID string
		runID      string
	}{
		{name: "other workflow, current run", workflowID: "target"},
		{name: "other workflow, explicit run", workflowID: "target", runID: targetRunID},
		{name: "own workflow, other run", workflowID: callerWorkflowID, runID: targetRunID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, scheduling := range []bool{true, false} {
				res, err := newResultProcessor(true).ProcessInput(resultProcessorContext(scheduling), resultRequest(tc.workflowID, tc.runID))
				require.NoError(t, err)
				// Namespace comes from the envelope; the request has no namespace field.
				require.Equal(t, chasm.NexusOperationRoutingKeyExecution{
					NamespaceID: tests.GlobalNamespaceEntry.ID().String(),
					BusinessID:  tc.workflowID,
				}, res.RoutingKey)
			}
		})
	}
}

func TestGetWorkflowExecutionResultProcessor_InvalidInputFailsOperation(t *testing.T) {
	noCaller := resultProcessorContext(false)
	noCaller.Caller = nil
	emptyCaller := resultProcessorContext(false)
	emptyCaller.Caller = &historyservice.NexusCallerInfo{}

	for _, tc := range []struct {
		name    string
		ctx     chasm.NexusOperationProcessorContext
		request *workflowservice.GetWorkflowExecutionResultRequest
		message string
	}{
		{name: "nil request", ctx: resultProcessorContext(true), message: "WorkflowId is not set"},
		{name: "nil execution", ctx: resultProcessorContext(true), request: &workflowservice.GetWorkflowExecutionResultRequest{}, message: "WorkflowId is not set"},
		{name: "empty workflow ID", ctx: resultProcessorContext(true), request: resultRequest("", targetRunID), message: "WorkflowId is not set"},
		{name: "malformed run ID", ctx: resultProcessorContext(true), request: resultRequest("target", "not-a-uuid"), message: "invalid RunId"},
		{name: "self wait on current run", ctx: resultProcessorContext(true), request: resultRequest(callerWorkflowID, ""), message: "cannot wait for its own result"},
		{name: "self wait on own run", ctx: resultProcessorContext(true), request: resultRequest(callerWorkflowID, callerRunID), message: "cannot wait for its own result"},
		{name: "invalid input at invocation", ctx: resultProcessorContext(false), request: resultRequest("", ""), message: "WorkflowId is not set"},
		{name: "standalone caller", ctx: noCaller, request: resultRequest("target", ""), message: "can only be called from a workflow"},
		{name: "empty caller", ctx: emptyCaller, request: resultRequest("target", ""), message: "can only be called from a workflow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newResultProcessor(true).ProcessInput(tc.ctx, tc.request)
			opErr, ok := errors.AsType[*nexus.OperationError](err)
			require.True(t, ok, "expected operation error, got %v", err)
			require.Equal(t, nexus.OperationStateFailed, opErr.State)
			require.NotNil(t, opErr.OriginalFailure, "HSM requires the original failure")

			// This is the failure the caller records on its NexusOperationFailed event.
			failure, err := commonnexus.OperationErrorToTemporalFailure(opErr)
			require.NoError(t, err)
			require.Contains(t, failure.GetMessage(), "InvalidArgument: ")
			require.Contains(t, failure.GetMessage(), tc.message)
		})
	}
}
