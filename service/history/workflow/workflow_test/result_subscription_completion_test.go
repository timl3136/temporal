package workflow_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type capturedHTTPCompletion struct {
	state     nexus.OperationState
	startTime time.Time
	closeTime time.Time
	links     []nexus.Link
	payload   *commonpb.Payload
	opErr     *nexus.OperationError
}

func (c *capturedHTTPCompletion) CompleteOperation(_ context.Context, req *nexusrpc.CompletionRequest) error {
	c.state = req.State
	c.startTime = req.StartTime
	c.closeTime = req.CloseTime
	c.links = req.Links
	if req.State == nexus.OperationStateSucceeded {
		var p *commonpb.Payload
		if err := req.Result.Consume(&p); err != nil {
			return err
		}
		c.payload = p
		return nil
	}
	c.opErr = req.Error
	return nil
}

func roundTripCompletionHTTP(t *testing.T, opts nexusrpc.CompleteOperationOptions) *capturedHTTPCompletion {
	t.Helper()

	captured := &capturedHTTPCompletion{}
	httpHandler := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
		Handler:    captured,
		Serializer: commonnexus.PayloadSerializer,
	})
	client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		HTTPCaller: func(req *http.Request) (*http.Response, error) {
			rec := httptest.NewRecorder()
			httpHandler.ServeHTTP(rec, req)
			return rec.Result(), nil
		},
		Serializer: commonnexus.PayloadSerializer,
	})
	require.NoError(t, client.CompleteOperation(t.Context(), "http://localhost/callback", opts))
	return captured
}

func decodeHTTPFailureAsCHASMAndHSM(
	t *testing.T,
	state nexus.OperationState,
	opErr *nexus.OperationError,
) (chasmFailure, hsmFailure *failurepb.Failure) {
	t.Helper()
	require.NotNil(t, opErr)
	require.NotNil(t, opErr.OriginalFailure)

	// CHASM caller path (service/frontend/nexus_completion_http_handler.go:completeChasmOperation).
	nf := nexusrpc.UnwrapFailure(opErr.OriginalFailure)
	chasmFailure, err := commonnexus.NexusFailureToTemporalFailure(*nf)
	require.NoError(t, err)
	if state == nexus.OperationStateCanceled {
		chasmFailure = commonnexus.CoerceToCanceledFailure(chasmFailure)
	}

	// HSM caller path (completeHSMOperation -> Handler.CompleteNexusOperation -> completion.go:handleOperationError).
	protoFailure := commonnexus.NexusFailureToProtoFailure(*opErr.OriginalFailure)
	recvdErr, err := nexusrpc.DefaultFailureConverter().FailureToError(commonnexus.ProtoFailureToNexusFailure(protoFailure))
	require.NoError(t, err)
	hsmOpErr, ok := recvdErr.(*nexus.OperationError)
	require.True(t, ok)
	require.Equal(t, state, hsmOpErr.State)
	if hsmOpErr.OriginalFailure.Metadata["unwrap-error"] == "true" && hsmOpErr.OriginalFailure.Cause != nil {
		hsmFailure, err = commonnexus.NexusFailureToTemporalFailure(*hsmOpErr.OriginalFailure.Cause)
	} else {
		hsmFailure, err = commonnexus.NexusFailureToTemporalFailure(*hsmOpErr.OriginalFailure)
	}
	require.NoError(t, err)
	if hsmOpErr.State == nexus.OperationStateCanceled {
		hsmFailure = commonnexus.CoerceToCanceledFailure(hsmFailure)
	}

	return chasmFailure, hsmFailure
}

func completeFirstWorkflowTask(t *testing.T, ms historyi.MutableState) {
	t.Helper()
	workflowTask, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	require.NoError(t, err)
	_, _, err = ms.AddWorkflowTaskStartedEvent(
		workflowTask.ScheduledEventID,
		"---",
		&taskqueuepb.TaskQueue{Name: "irrelevant"},
		"---",
		nil,
		nil,
		nil,
		false,
		nil,
		0,
		"",
	)
	require.NoError(t, err)
	_, err = ms.AddWorkflowTaskCompletedEvent(workflowTask, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some-worker",
	}, historyi.WorkflowTaskCompletionLimits{MaxResetPoints: 10, MaxSearchAttributeValueSize: 10})
	require.NoError(t, err)
}

func TestGetResultSubscriptionCompletion_FixturesAndLegacyIsolation(t *testing.T) {
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
	details := &commonpb.Payloads{Payloads: []*commonpb.Payload{{
		Metadata: map[string][]byte{"encoding": []byte("binary/encrypted"), "codec": {0x00, 0xff}},
		Data:     []byte{0x80, 0x00, 0xff},
	}}}
	originalFailure := &failurepb.Failure{
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
	wantCanceled := &failurepb.Failure{
		Message: "workflow canceled",
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{Details: details},
		},
	}
	wantTerminated := &failurepb.Failure{
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
	wantTimedOut := &failurepb.Failure{
		Message: "workflow timeout",
		Source:  "Server",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE},
		},
	}

	cases := []struct {
		name               string
		blobLimit          int
		closeWorkflow      func(historyi.MutableState) (*historypb.HistoryEvent, error)
		wantPayload        *commonpb.Payload
		wantAbsentPayload  bool
		wantState          nexus.OperationState
		wantFailure        *failurepb.Failure
		wantRejectionMsg   string
		verifyLegacyOnSame func(*testing.T, nexusrpc.CompleteOperationOptions)
	}{
		{
			name:      "completed absent payload",
			blobLimit: 1024,
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, "")
			},
			wantAbsentPayload: true,
			verifyLegacyOnSame: func(t *testing.T, legacy nexusrpc.CompleteOperationOptions) {
				require.Nil(t, legacy.Error)
				require.Nil(t, legacy.Result)
			},
		},
		{
			name:      "completed present empty payload",
			blobLimit: 1024,
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{}}},
				}, "")
			},
			wantPayload: &commonpb.Payload{},
		},
		{
			name:      "completed encrypted payload",
			blobLimit: 1024,
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{stored}},
				}, "")
			},
			wantPayload: stored,
		},
		{
			name:      "completed arbitrary binary metadata",
			blobLimit: 1024,
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{withBinaryMetadata}},
				}, "")
			},
			wantPayload: withBinaryMetadata,
		},
		{
			name:      "completed two payloads rejected for subscription but kept for legacy",
			blobLimit: 1024,
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{stored, withBinaryMetadata}},
				}, "")
			},
			wantState:        nexus.OperationStateFailed,
			wantRejectionMsg: "multiple",
			verifyLegacyOnSame: func(t *testing.T, legacy nexusrpc.CompleteOperationOptions) {
				require.Nil(t, legacy.Error)
				protorequire.ProtoEqual(t, stored, legacy.Result.(*commonpb.Payload))
			},
		},
		{
			name:      "completed at limit",
			blobLimit: proto.Size(large),
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{large}},
				}, "")
			},
			wantPayload: large,
		},
		{
			name:      "completed over limit",
			blobLimit: proto.Size(large) - 1,
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{large}},
				}, "")
			},
			wantState:        nexus.OperationStateFailed,
			wantRejectionMsg: "size limit",
		},
		{
			name:      "failed",
			blobLimit: proto.Size(originalFailure),
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddFailWorkflowEvent(ms.GetNextEventID(), enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: originalFailure,
				}, "")
			},
			wantState:   nexus.OperationStateFailed,
			wantFailure: originalFailure,
		},
		{
			name:      "failed oversized nested",
			blobLimit: proto.Size(originalFailure) - 1,
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddFailWorkflowEvent(ms.GetNextEventID(), enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: originalFailure,
				}, "")
			},
			wantState:        nexus.OperationStateFailed,
			wantRejectionMsg: "size limit",
		},
		{
			name:      "canceled",
			blobLimit: proto.Size(wantCanceled),
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddWorkflowExecutionCanceledEvent(ms.GetNextEventID(), &commandpb.CancelWorkflowExecutionCommandAttributes{
					Details: details,
				})
			},
			wantState:   nexus.OperationStateCanceled,
			wantFailure: wantCanceled,
			verifyLegacyOnSame: func(t *testing.T, legacy nexusrpc.CompleteOperationOptions) {
				require.NotNil(t, legacy.Error)
				require.Equal(t, nexus.OperationStateCanceled, legacy.Error.State)
				legacyFail, err := commonnexus.OperationErrorToTemporalFailure(legacy.Error)
				require.NoError(t, err)
				require.Equal(t, "operation canceled", legacyFail.GetMessage())
			},
		},
		{
			name:      "terminated preserves reason identity and details unlike legacy",
			blobLimit: proto.Size(wantTerminated),
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddWorkflowExecutionTerminatedEvent("termination reason", details, "operator", false, nil)
			},
			wantState:   nexus.OperationStateFailed,
			wantFailure: wantTerminated,
			verifyLegacyOnSame: func(t *testing.T, legacy nexusrpc.CompleteOperationOptions) {
				require.NotNil(t, legacy.Error)
				require.Equal(t, nexus.OperationStateFailed, legacy.Error.State)
				legacyFail, err := commonnexus.OperationErrorToTemporalFailure(legacy.Error)
				require.NoError(t, err)
				require.Equal(t, "operation terminated", legacyFail.GetMessage())
				require.Empty(t, legacyFail.GetTerminatedFailureInfo().GetIdentity())
				require.Nil(t, legacyFail.GetCause())
			},
		},
		{
			name:      "timed out preserves timeout type unlike legacy",
			blobLimit: proto.Size(wantTimedOut),
			closeWorkflow: func(ms historyi.MutableState) (*historypb.HistoryEvent, error) {
				return ms.AddTimeoutWorkflowEvent(enumspb.RETRY_STATE_TIMEOUT, "")
			},
			wantState:   nexus.OperationStateFailed,
			wantFailure: wantTimedOut,
			verifyLegacyOnSame: func(t *testing.T, legacy nexusrpc.CompleteOperationOptions) {
				require.NotNil(t, legacy.Error)
				require.Equal(t, nexus.OperationStateFailed, legacy.Error.State)
				legacyFail, err := commonnexus.OperationErrorToTemporalFailure(legacy.Error)
				require.NoError(t, err)
				require.Equal(t, "operation exceeded internal timeout", legacyFail.GetMessage())
				require.Equal(t, enumspb.TIMEOUT_TYPE_UNSPECIFIED, legacyFail.GetTimeoutFailureInfo().GetTimeoutType())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nsEntry := tests.LocalNamespaceEntry
			cfg := tests.NewDynamicConfig()
			cfg.BlobSizeLimitError = dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.blobLimit)
			ms, eventsCache, _ := createMutableState(t, nsEntry, cfg)
			startWorkflowExecution(t, ms, nsEntry)
			completeFirstWorkflowTask(t, ms)

			event, err := tc.closeWorkflow(ms)
			require.NoError(t, err)
			sealMutableState(t, ms, event)

			// Expect one read for GetResultSubscriptionCompletion, and another when verifying legacy on the same target.
			reads := 1
			if tc.verifyLegacyOnSame != nil {
				reads = 2
			}
			eventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(event, nil).Times(reads)

			wfKey := ms.GetWorkflowKey()
			sub := &commonpb.Callback_ResultSubscription{
				RequestId:           "sub-req-1",
				ResultFormatVersion: 1,
				ResolvedExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfKey.WorkflowID,
					RunId:      "resolved-origin-run-id",
				},
			}
			// Simulate an inherited callback whose run-local request ID differs from sub.RequestId.
			completion, err := ms.GetResultSubscriptionCompletion(t.Context(), "can-start-req-id", sub)
			require.NoError(t, err)
			require.Equal(t, event.GetEventTime().AsTime(), completion.CloseTime)
			require.Equal(t, ms.GetExecutionState().GetStartTime().AsTime(), completion.StartTime)

			// Verify request-ID link retains the original attach execution and request ID.
			require.Len(t, completion.Links, 1)
			gotLink, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(completion.Links[0])
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &commonpb.Link_WorkflowEvent{
				Namespace:  nsEntry.Name().String(),
				WorkflowId: wfKey.WorkflowID,
				RunId:      "resolved-origin-run-id",
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: "sub-req-1",
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
					},
				},
			}, gotLink)

			// Round-trip through the real Nexus completion HTTP client + handler.
			wire := roundTripCompletionHTTP(t, completion)
			require.Equal(t, completion.CloseTime.UTC(), wire.closeTime.UTC())
			require.Len(t, wire.links, 1)
			wireLink, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(wire.links[0])
			require.NoError(t, err)
			protorequire.ProtoEqual(t, gotLink, wireLink)

			if tc.wantRejectionMsg != "" {
				require.Nil(t, completion.Result)
				require.NotNil(t, completion.Error)
				require.Equal(t, tc.wantState, completion.Error.State)

				syncFailure, err := commonnexus.OperationErrorToTemporalFailure(completion.Error)
				require.NoError(t, err)
				require.Contains(t, syncFailure.GetMessage(), tc.wantRejectionMsg)
				require.True(t, syncFailure.GetServerFailureInfo().GetNonRetryable())
				require.Nil(t, syncFailure.GetCause())
				require.Nil(t, syncFailure.GetEncodedAttributes())
				require.LessOrEqual(t, proto.Size(syncFailure), tc.blobLimit)

				chasmFailure, hsmFailure := decodeHTTPFailureAsCHASMAndHSM(t, wire.state, wire.opErr)
				protorequire.ProtoEqual(t, syncFailure, chasmFailure)
				protorequire.ProtoEqual(t, syncFailure, hsmFailure)
			} else if tc.wantFailure != nil {
				require.Nil(t, completion.Result)
				require.NotNil(t, completion.Error)
				require.Equal(t, tc.wantState, completion.Error.State)

				syncFailure, err := commonnexus.OperationErrorToTemporalFailure(completion.Error)
				require.NoError(t, err)
				protorequire.ProtoEqual(t, tc.wantFailure, syncFailure)

				chasmFailure, hsmFailure := decodeHTTPFailureAsCHASMAndHSM(t, wire.state, wire.opErr)
				protorequire.ProtoEqual(t, syncFailure, chasmFailure)
				protorequire.ProtoEqual(t, syncFailure, hsmFailure)
			} else if tc.wantAbsentPayload {
				require.Nil(t, completion.Error)
				require.Nil(t, completion.Result)
				require.Equal(t, nexus.OperationStateSucceeded, wire.state)
				// Over HTTP, PayloadSerializer represents nil as binary/null.
				wantNull, err := payload.Encode(nil)
				require.NoError(t, err)
				protorequire.ProtoEqual(t, wantNull, wire.payload)
			} else {
				require.Nil(t, completion.Error)
				gotPayload, ok := completion.Result.(*commonpb.Payload)
				require.True(t, ok)
				protorequire.ProtoEqual(t, tc.wantPayload, gotPayload)
				require.NotSame(t, tc.wantPayload, gotPayload)

				require.Equal(t, nexus.OperationStateSucceeded, wire.state)
				protorequire.ProtoEqual(t, tc.wantPayload, wire.payload)
			}

			if tc.verifyLegacyOnSame != nil {
				legacy, err := ms.GetNexusCompletion(t.Context(), "legacy-req-id")
				require.NoError(t, err)
				tc.verifyLegacyOnSame(t, legacy)
			}
		})
	}
}

func TestGetResultSubscriptionCompletion_Errors(t *testing.T) {
	t.Parallel()

	t.Run("unsupported format versions and missing metadata", func(t *testing.T) {
		t.Parallel()
		nsEntry := tests.LocalNamespaceEntry
		ms, _, _ := createMutableState(t, nsEntry, tests.NewDynamicConfig())
		startWorkflowExecution(t, ms, nsEntry)

		for _, tc := range []struct {
			name string
			sub  *commonpb.Callback_ResultSubscription
		}{
			{name: "nil subscription"},
			{name: "version 0", sub: &commonpb.Callback_ResultSubscription{RequestId: "r1", ResultFormatVersion: 0}},
			{name: "version 2", sub: &commonpb.Callback_ResultSubscription{RequestId: "r1", ResultFormatVersion: 2}},
			{name: "empty request ID", sub: &commonpb.Callback_ResultSubscription{ResultFormatVersion: 1}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := ms.GetResultSubscriptionCompletion(t.Context(), "r1", tc.sub)
				var internalErr *serviceerror.Internal
				require.ErrorAs(t, err, &internalErr)
			})
		}
	})

	t.Run("transient completion event read error propagates", func(t *testing.T) {
		t.Parallel()
		nsEntry := tests.LocalNamespaceEntry
		ms, eventsCache, _ := createMutableState(t, nsEntry, tests.NewDynamicConfig())
		startWorkflowExecution(t, ms, nsEntry)
		completeFirstWorkflowTask(t, ms)

		event, err := ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, "")
		require.NoError(t, err)
		sealMutableState(t, ms, event)

		transientErr := serviceerror.NewUnavailable("persistence unavailable")
		eventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, transientErr).Times(1)

		sub := &commonpb.Callback_ResultSubscription{
			RequestId:           "sub-req-1",
			ResultFormatVersion: 1,
		}
		_, err = ms.GetResultSubscriptionCompletion(t.Context(), "sub-req-1", sub)
		require.ErrorIs(t, err, transientErr)
	})

	t.Run("direct attach link fallback when resolved execution absent", func(t *testing.T) {
		t.Parallel()
		nsEntry := tests.LocalNamespaceEntry
		ms, eventsCache, _ := createMutableState(t, nsEntry, tests.NewDynamicConfig())
		startWorkflowExecution(t, ms, nsEntry)
		completeFirstWorkflowTask(t, ms)

		_, err := ms.AddWorkflowExecutionOptionsUpdatedEvent(
			nil, false, "direct-attach-req", nil, nil, "", nil, nil, false, nil,
		)
		require.NoError(t, err)
		event, err := ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, "")
		require.NoError(t, err)
		sealMutableState(t, ms, event)

		eventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(event, nil).Times(1)

		sub := &commonpb.Callback_ResultSubscription{
			RequestId:           "direct-attach-req",
			ResultFormatVersion: 1,
		}
		completion, err := ms.GetResultSubscriptionCompletion(t.Context(), "direct-attach-req", sub)
		require.NoError(t, err)
		require.Len(t, completion.Links, 1)
		gotLink, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(completion.Links[0])
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &commonpb.Link_WorkflowEvent{
			Namespace:  nsEntry.Name().String(),
			WorkflowId: ms.GetWorkflowKey().WorkflowID,
			RunId:      ms.GetWorkflowKey().RunID,
			Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
				RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
					RequestId: "direct-attach-req",
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				},
			},
		}, gotLink)
	})

	t.Run("non-terminal close event pointing to successor returns internal error", func(t *testing.T) {
		t.Parallel()
		nsEntry := tests.LocalNamespaceEntry
		ms, eventsCache, _ := createMutableState(t, nsEntry, tests.NewDynamicConfig())
		startWorkflowExecution(t, ms, nsEntry)
		completeFirstWorkflowTask(t, ms)

		event, err := ms.AddCompletedWorkflowEvent(ms.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, "next-run-id")
		require.NoError(t, err)
		sealMutableState(t, ms, event)

		eventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(event, nil).Times(1)

		sub := &commonpb.Callback_ResultSubscription{
			RequestId:           "sub-req-1",
			ResultFormatVersion: 1,
		}
		_, err = ms.GetResultSubscriptionCompletion(t.Context(), "sub-req-1", sub)
		var internalErr *serviceerror.Internal
		require.ErrorAs(t, err, &internalErr)
	})
}
