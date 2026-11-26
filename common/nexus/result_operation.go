package nexus

import (
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
)

// IsGetWorkflowExecutionResult reports whether a scheduled operation is the system GetWorkflowExecutionResult
// operation. Its explicit cancel removes the caller's subscription instead of canceling the target workflow, so the
// target never reports the cancel back and the caller records Canceled itself.
func IsGetWorkflowExecutionResult(endpoint, service, operation string) bool {
	svc := workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService
	return endpoint == SystemEndpoint && service == svc.ServiceName && operation == svc.GetWorkflowExecutionResult.Name()
}

// ResultSubscriptionCanceledFailure is the cause the caller records when it cancels a GetWorkflowExecutionResult
// subscription.
func ResultSubscriptionCanceledFailure() *failurepb.Failure {
	return CoerceToCanceledFailure(&failurepb.Failure{Message: "operation canceled"})
}
