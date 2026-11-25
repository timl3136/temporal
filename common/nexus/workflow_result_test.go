package nexus

import (
	"strings"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func TestWorkflowResultFromCloseEventSuccess(t *testing.T) {
	t.Parallel()

	stored := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/encrypted")},
		Data:     []byte{0xff, 0x00, 0x80},
	}
	withBinaryMetadata := &commonpb.Payload{
		Metadata: map[string][]byte{"codec": {0x00, 0x80, 0xff}, "encoding": []byte("binary/encrypted")},
		Data:     stored.Data,
	}
	large := &commonpb.Payload{
		Metadata: map[string][]byte{"codec": make([]byte, 256)},
		Data:     stored.Data,
	}

	for _, tc := range []struct {
		name      string
		values    []*commonpb.Payload
		limit     int
		want      *commonpb.Payload
		rejection string
	}{
		{name: "absent", limit: 1024},
		{name: "present empty payload", values: []*commonpb.Payload{{}}, limit: 1024, want: &commonpb.Payload{}},
		{name: "encrypted payload", values: []*commonpb.Payload{stored}, limit: 1024, want: stored},
		{name: "arbitrary binary metadata", values: []*commonpb.Payload{withBinaryMetadata}, limit: 1024, want: withBinaryMetadata},
		{name: "two values", values: []*commonpb.Payload{stored, stored}, limit: 1024, rejection: "multiple"},
		{name: "at limit", values: []*commonpb.Payload{large}, limit: proto.Size(large), want: large},
		{name: "over limit", values: []*commonpb.Payload{large}, limit: proto.Size(large) - 1, rejection: "size limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			event := &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
						Result: &commonpb.Payloads{Payloads: tc.values},
					},
				},
			}

			got, opErr, err := WorkflowResultFromCloseEvent(event, tc.limit)
			require.NoError(t, err)
			if tc.rejection != "" {
				require.Nil(t, got)
				require.NotNil(t, opErr)
				require.Equal(t, nexus.OperationStateFailed, opErr.State)
				failure, err := OperationErrorToTemporalFailure(opErr)
				require.NoError(t, err)
				require.Contains(t, failure.GetMessage(), tc.rejection)
				require.True(t, failure.GetServerFailureInfo().GetNonRetryable())
				require.Nil(t, failure.GetCause())
				require.LessOrEqual(t, proto.Size(failure), tc.limit)
				return
			}
			require.Nil(t, opErr)
			require.True(t, proto.Equal(tc.want, got))
			if tc.want != nil {
				require.NotSame(t, tc.want, got)
			}
		})
	}
}

func TestWorkflowResultFromCloseEventFailure(t *testing.T) {
	t.Parallel()

	details := &commonpb.Payloads{Payloads: []*commonpb.Payload{{
		Metadata: map[string][]byte{"encoding": []byte("binary/encrypted"), "codec": {0x00, 0xff}},
		Data:     []byte{0x80, 0x00, 0xff},
	}}}
	original := &failurepb.Failure{
		Message:           "workflow failed",
		StackTrace:        "stack trace",
		EncodedAttributes: details.Payloads[0],
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "Example", Details: details},
		},
		Cause: &failurepb.Failure{
			Message: strings.Repeat("nested cause ", 24),
		},
	}
	canceled := &failurepb.Failure{
		Message: "workflow canceled",
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{Details: details},
		},
	}
	terminated := &failurepb.Failure{
		Message: "termination reason",
		FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
			TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{Identity: "operator"},
		},
		Cause: &failurepb.Failure{
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Details: details},
			},
		},
	}
	timedOut := &failurepb.Failure{
		Message: "workflow timeout",
		Source:  "Server",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE},
		},
	}

	failedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{Failure: original},
		},
	}
	for _, tc := range []struct {
		name  string
		event *historypb.HistoryEvent
		state nexus.OperationState
		want  *failurepb.Failure
	}{
		{name: "failed", event: failedEvent, state: nexus.OperationStateFailed, want: original},
		{
			name: "canceled",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
					WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{Details: details},
				},
			},
			state: nexus.OperationStateCanceled,
			want:  canceled,
		},
		{
			name: "terminated",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
					WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
						Reason: "termination reason", Identity: "operator", Details: details,
					},
				},
			},
			state: nexus.OperationStateFailed,
			want:  terminated,
		},
		{
			name: "timed out",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
					WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{},
				},
			},
			state: nexus.OperationStateFailed,
			want:  timedOut,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, opErr, err := WorkflowResultFromCloseEvent(tc.event, proto.Size(tc.want))
			require.NoError(t, err)
			require.Nil(t, got)
			require.NotNil(t, opErr)
			require.Equal(t, tc.state, opErr.State)
			failure, err := OperationErrorToTemporalFailure(opErr)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.want, failure)
		})
	}

	t.Run("oversized nested failure", func(t *testing.T) {
		t.Parallel()

		limit := proto.Size(original) - 1
		got, opErr, err := WorkflowResultFromCloseEvent(failedEvent, limit)
		require.NoError(t, err)
		require.Nil(t, got)
		require.NotNil(t, opErr)
		require.Equal(t, nexus.OperationStateFailed, opErr.State)
		failure, err := OperationErrorToTemporalFailure(opErr)
		require.NoError(t, err)
		require.Contains(t, failure.GetMessage(), "size limit")
		require.True(t, failure.GetServerFailureInfo().GetNonRetryable())
		require.Nil(t, failure.GetCause())
		require.Nil(t, failure.GetEncodedAttributes())
		require.LessOrEqual(t, proto.Size(failure), limit)
	})
}

func TestWorkflowResultFromCloseEventRequiresTerminalEvent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		event *historypb.HistoryEvent
	}{
		{name: "nil"},
		{name: "missing attributes", event: &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED}},
		{name: "open", event: &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}},
		{
			name: "successor",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
						NewExecutionRunId: "successor",
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, opErr, err := WorkflowResultFromCloseEvent(tc.event, 1024)
			require.ErrorAs(t, err, new(*serviceerror.Internal))
			require.Nil(t, got)
			require.Nil(t, opErr)
		})
	}
}
