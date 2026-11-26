package callbacks_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/backoff"
	commoncallbacks "go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/callbacks"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dualMutableState struct {
	legacyCompletion nexusrpc.CompleteOperationOptions
	subCompletion    nexusrpc.CompleteOperationOptions
	subErr           error
	legacyCalls      []string
	subCalls         []struct {
		requestID string
		sub       *commonpb.Callback_ResultSubscription
	}
}

func (ms *dualMutableState) GetNexusCompletion(_ context.Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	ms.legacyCalls = append(ms.legacyCalls, requestID)
	return ms.legacyCompletion, nil
}

func (ms *dualMutableState) GetResultSubscriptionCompletion(
	_ context.Context,
	requestID string,
	sub *commonpb.Callback_ResultSubscription,
) (nexusrpc.CompleteOperationOptions, error) {
	ms.subCalls = append(ms.subCalls, struct {
		requestID string
		sub       *commonpb.Callback_ResultSubscription
	}{requestID: requestID, sub: sub})
	return ms.subCompletion, ms.subErr
}

func newHSMTestNamespace(t *testing.T) *namespace.Namespace {
	t.Helper()
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   "namespace-id",
			Name: "namespace-name",
		},
		Config: &persistencespb.NamespaceConfig{},
	}
	ns, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)
	return ns
}

func executeHSMCallbackTask(
	t *testing.T,
	parentData any,
	cbID string,
	cb callbacks.Callback,
	destination string,
	caller callbacks.HTTPCaller,
	historyClient *historyservicemock.MockHistoryServiceClient,
	logger log.Logger,
) (callbacks.Callback, error) {
	t.Helper()

	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).Return(newHSMTestNamespace(t), nil)

	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterExecutor(reg, callbacks.TaskExecutorOptions{
		NamespaceRegistry: nsRegistry,
		MetricsHandler:    metrics.NoopMetricsHandler,
		HTTPCallerProvider: func(queuescommon.NamespaceIDAndDestination) callbacks.HTTPCaller {
			return caller
		},
		HistoryClient: historyClient,
		Logger:        logger,
		Config: &callbacks.Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
	}))

	root, err := hsm.NewRoot(reg, workflow.StateMachineType, parentData, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	coll := callbacks.MachineCollection(root)
	node, err := coll.Add(cbID, cb)
	require.NoError(t, err)

	err = reg.ExecuteImmediateTask(
		t.Context(),
		fakeEnv{node},
		hsm.Ref{
			WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id"),
			StateMachineRef: &persistencespb.StateMachineRef{
				Path: []*persistencespb.StateMachineKey{{
					Type: callbacks.StateMachineType,
					Id:   cbID,
				}},
			},
		},
		callbacks.NewInvocationTask(destination),
	)
	updated, dataErr := coll.Data(cbID)
	require.NoError(t, dataErr)
	return updated, err
}

func TestProcessInvocationTask_ResultSubscriptionValidation(t *testing.T) {
	t.Parallel()

	validSub := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: 1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "origin-run"},
	}

	cases := []struct {
		name              string
		url               string
		sub               *commonpb.Callback_ResultSubscription
		useLegacyOnlyHost bool
		wantErr           string
	}{
		{
			name: "format version 0 rejected",
			url:  commonnexus.SystemCallbackURL,
			sub: &commonpb.Callback_ResultSubscription{
				RequestId:           "sub-req-1",
				ResultFormatVersion: 0,
			},
			wantErr: "unsupported result subscription format version: 0",
		},
		{
			name: "format version 2 rejected",
			url:  commonnexus.SystemCallbackURL,
			sub: &commonpb.Callback_ResultSubscription{
				RequestId:           "sub-req-1",
				ResultFormatVersion: 2,
			},
			wantErr: "unsupported result subscription format version: 2",
		},
		{
			name: "missing subscription request ID rejected",
			url:  commonnexus.SystemCallbackURL,
			sub: &commonpb.Callback_ResultSubscription{
				ResultFormatVersion: 1,
			},
			wantErr: "result subscription callback missing request ID",
		},
		{
			name:    "third-party URL rejected",
			url:     "https://third-party.example/cb",
			sub:     validSub,
			wantErr: "unsupported callback URL for result subscription",
		},
		{
			name:              "parent without result subscription support rejected",
			url:               commonnexus.SystemCallbackURL,
			sub:               validSub,
			useLegacyOnlyHost: true,
			wantErr:           "does not support result subscriptions",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var parent any = &dualMutableState{}
			if tc.useLegacyOnlyHost {
				parent = mutableState{}
			}
			cb := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					Callback: &persistencespb.Callback{
						Variant: &persistencespb.Callback_Nexus_{
							Nexus: &persistencespb.Callback_Nexus{Url: tc.url},
						},
						ResultSubscription: tc.sub,
					},
					State:     enumsspb.CALLBACK_STATE_SCHEDULED,
					RequestId: "attach-req-1",
				},
			}

			_, err := executeHSMCallbackTask(t, parent, "cb-1", cb, tc.url, nil, nil, log.NewTestLogger())
			var unprocessableErr *queueserrors.UnprocessableTaskError
			require.ErrorAs(t, err, &unprocessableErr)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestProcessInvocationTask_ResultSubscriptionAndLegacySameTarget(t *testing.T) {
	t.Parallel()

	subPayload := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/encrypted")},
		Data:     []byte{0xff, 0x00, 0x80},
	}
	legacyPayload := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(`"legacy"`),
	}
	subMeta := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: 1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "origin-run"},
	}

	ms := &dualMutableState{
		legacyCompletion: nexusrpc.CompleteOperationOptions{Result: legacyPayload},
		subCompletion:    nexusrpc.CompleteOperationOptions{Result: subPayload},
	}

	received := make(map[string]*commonpb.Payload)
	caller := func(req *http.Request) (*http.Response, error) {
		var p *commonpb.Payload
		h := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
			Serializer: commonnexus.PayloadSerializer,
			Handler: hsmCompletionHandlerFunc(func(_ context.Context, comp *nexusrpc.CompletionRequest) error {
				return comp.Result.Consume(&p)
			}),
		})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		received[req.URL.String()] = p
		return rec.Result(), nil
	}

	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).Return(newHSMTestNamespace(t), nil).Times(2)

	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterExecutor(reg, callbacks.TaskExecutorOptions{
		NamespaceRegistry: nsRegistry,
		MetricsHandler:    metrics.NoopMetricsHandler,
		HTTPCallerProvider: func(queuescommon.NamespaceIDAndDestination) callbacks.HTTPCaller {
			return caller
		},
		Logger: log.NewTestLogger(),
		Config: &callbacks.Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
	}))

	root, err := hsm.NewRoot(reg, workflow.StateMachineType, ms, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	coll := callbacks.MachineCollection(root)
	subNode, err := coll.Add("sub", callbacks.Callback{
		CallbackInfo: &persistencespb.CallbackInfo{
			Callback: &persistencespb.Callback{
				Variant: &persistencespb.Callback_Nexus_{
					Nexus: &persistencespb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
				},
				ResultSubscription: subMeta,
			},
			State:     enumsspb.CALLBACK_STATE_SCHEDULED,
			RequestId: "can-req-1",
		},
	})
	require.NoError(t, err)
	legacyNode, err := coll.Add("legacy", callbacks.Callback{
		CallbackInfo: &persistencespb.CallbackInfo{
			Callback: &persistencespb.Callback{
				Variant: &persistencespb.Callback_Nexus_{
					Nexus: &persistencespb.Callback_Nexus{Url: "http://third-party.example/cb"},
				},
			},
			State:     enumsspb.CALLBACK_STATE_SCHEDULED,
			RequestId: "legacy-req-1",
		},
	})
	require.NoError(t, err)

	key := definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")
	require.NoError(t, reg.ExecuteImmediateTask(
		t.Context(),
		fakeEnv{subNode},
		hsm.Ref{
			WorkflowKey: key,
			StateMachineRef: &persistencespb.StateMachineRef{
				Path: []*persistencespb.StateMachineKey{{Type: callbacks.StateMachineType, Id: "sub"}},
			},
		},
		callbacks.NewInvocationTask(commonnexus.SystemCallbackURL),
	))
	require.NoError(t, reg.ExecuteImmediateTask(
		t.Context(),
		fakeEnv{legacyNode},
		hsm.Ref{
			WorkflowKey: key,
			StateMachineRef: &persistencespb.StateMachineRef{
				Path: []*persistencespb.StateMachineKey{{Type: callbacks.StateMachineType, Id: "legacy"}},
			},
		},
		callbacks.NewInvocationTask("http://third-party.example"),
	))

	require.Len(t, ms.subCalls, 1)
	require.Equal(t, "can-req-1", ms.subCalls[0].requestID)
	protorequire.ProtoEqual(t, subMeta, ms.subCalls[0].sub)
	require.Equal(t, []string{"legacy-req-1"}, ms.legacyCalls)

	protorequire.ProtoEqual(t, subPayload, received[commonnexus.SystemCallbackURL])
	protorequire.ProtoEqual(t, legacyPayload, received["http://third-party.example/cb"])
}

func TestProcessInvocationTask_ResultSubscription_RetryAndPermanentErrors(t *testing.T) {
	t.Parallel()

	subMeta := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: 1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "origin-run"},
	}

	t.Run("system HTTP delivery", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name        string
			statusCode  int
			wantState   enumsspb.CallbackState
			wantDestErr bool
		}{
			{name: "success", statusCode: http.StatusOK, wantState: enumsspb.CALLBACK_STATE_SUCCEEDED},
			{name: "retryable 500 backs off", statusCode: http.StatusInternalServerError, wantState: enumsspb.CALLBACK_STATE_BACKING_OFF, wantDestErr: true},
			{name: "permanent 404 marks failed", statusCode: http.StatusNotFound, wantState: enumsspb.CALLBACK_STATE_FAILED},
			{name: "permanent 400 marks failed", statusCode: http.StatusBadRequest, wantState: enumsspb.CALLBACK_STATE_FAILED},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
				capture := logger.StartCapture()
				cb := callbacks.Callback{
					CallbackInfo: &persistencespb.CallbackInfo{
						Callback: &persistencespb.Callback{
							Variant: &persistencespb.Callback_Nexus_{
								Nexus: &persistencespb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
							},
							ResultSubscription: subMeta,
						},
						State:     enumsspb.CALLBACK_STATE_SCHEDULED,
						RequestId: "can-req-1",
					},
				}
				updated, err := executeHSMCallbackTask(t, &dualMutableState{}, "sub", cb, commonnexus.SystemCallbackURL, func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: tc.statusCode, Body: http.NoBody}, nil
				}, nil, logger)
				if tc.wantDestErr {
					var destDownErr *queueserrors.DestinationDownError
					require.ErrorAs(t, err, &destDownErr)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.wantState, updated.State())
				if tc.statusCode != http.StatusOK {
					capture.RequireContains(t, testlogger.CapturedLogPattern{
						Level:   testlogger.Error,
						Message: "Callback request failed",
						Tags: map[string]any{
							"request-id": "sub-req-1",
						},
					})
				}
			})
		}
	})

	t.Run("internal CHASM delivery", func(t *testing.T) {
		t.Parallel()
		refBytes, err := (&persistencespb.ChasmComponentRef{
			NamespaceId: "namespace-id",
			BusinessId:  "caller-wf",
			RunId:       "caller-run",
			ArchetypeId: 1234,
		}).Marshal()
		require.NoError(t, err)
		encodedToken := base64.RawURLEncoding.EncodeToString(refBytes)

		for _, tc := range []struct {
			name      string
			rpcErr    error
			wantState enumsspb.CallbackState
		}{
			{name: "success", wantState: enumsspb.CALLBACK_STATE_SUCCEEDED},
			{name: "retryable unavailable backs off", rpcErr: status.Error(codes.Unavailable, "unavailable"), wantState: enumsspb.CALLBACK_STATE_BACKING_OFF},
			{name: "permanent not found marks failed", rpcErr: status.Error(codes.NotFound, "gone"), wantState: enumsspb.CALLBACK_STATE_FAILED},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ctrl := gomock.NewController(t)
				historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
				historyClient.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *historyservice.CompleteNexusOperationChasmRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
						require.Equal(t, "sub-req-1", req.GetCompletion().GetRequestId())
						if tc.rpcErr != nil {
							return nil, tc.rpcErr
						}
						return &historyservice.CompleteNexusOperationChasmResponse{}, nil
					},
				)
				cb := callbacks.Callback{
					CallbackInfo: &persistencespb.CallbackInfo{
						Callback: &persistencespb.Callback{
							Variant: &persistencespb.Callback_Nexus_{
								Nexus: &persistencespb.Callback_Nexus{
									Url:    chasm.NexusCompletionHandlerURL,
									Header: map[string]string{strings.ToLower(commonnexus.CallbackTokenHeader): encodedToken},
								},
							},
							ResultSubscription: subMeta,
						},
						State:     enumsspb.CALLBACK_STATE_SCHEDULED,
						RequestId: "can-req-1",
					},
				}
				updated, err := executeHSMCallbackTask(t, &dualMutableState{}, "sub", cb, chasm.NexusCompletionHandlerURL, nil, historyClient, log.NewTestLogger())
				if tc.rpcErr != nil {
					require.ErrorContains(t, err, "internal error, reference-id:")
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.wantState, updated.State())
			})
		}
	})
}

func TestProcessInvocationTask_ResultSubscription_WireParity(t *testing.T) {
	t.Parallel()

	stored := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/encrypted"), "codec": {0x80, 0x00, 0xff}},
		Data:     []byte{0xff, 0x00, 0x80},
	}
	details := &commonpb.Payloads{Payloads: []*commonpb.Payload{stored}}
	closeTime := time.Date(2026, 11, 25, 12, 30, 0, 0, time.UTC)
	startTime := closeTime.Add(-time.Minute)
	attachLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
		Namespace:  "namespace-name",
		WorkflowId: "workflow-id",
		RunId:      "origin-run",
		Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
			RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
				RequestId: "sub-req-1",
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			},
		},
	})
	subMeta := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: 1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "origin-run"},
	}

	makeWrappedOpErr := func(t *testing.T, state nexus.OperationState, cause *failurepb.Failure) *nexus.OperationError {
		t.Helper()
		nf, err := commonnexus.TemporalFailureToNexusFailure(cause)
		require.NoError(t, err)
		opErr := &nexus.OperationError{State: state, Cause: &nexus.FailureError{Failure: nf}}
		require.NoError(t, nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr))
		return opErr
	}

	failedCause := &failurepb.Failure{
		Message:           "workflow failed",
		StackTrace:        "stack trace",
		EncodedAttributes: stored,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "Example", Details: details},
		},
		Cause: &failurepb.Failure{Message: "nested cause"},
	}
	canceledCause := &failurepb.Failure{
		Message: "workflow canceled",
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{Details: details},
		},
	}
	terminatedCause := &failurepb.Failure{
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
	timedOutCause := &failurepb.Failure{
		Message: "workflow timeout",
		Source:  "Server",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE},
		},
	}

	refBytes, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: "namespace-id",
		BusinessId:  "caller-wf",
		RunId:       "caller-run",
		ArchetypeId: 1234,
	}).Marshal()
	require.NoError(t, err)
	encodedToken := base64.RawURLEncoding.EncodeToString(refBytes)

	for _, tc := range []struct {
		name    string
		result  *commonpb.Payload
		failure *failurepb.Failure
		state   nexus.OperationState
	}{
		{name: "absent payload", result: &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/null")}}, state: nexus.OperationStateSucceeded},
		{name: "present empty payload", result: &commonpb.Payload{}, state: nexus.OperationStateSucceeded},
		{name: "raw encrypted payload", result: stored, state: nexus.OperationStateSucceeded},
		{name: "failed", failure: failedCause, state: nexus.OperationStateFailed},
		{name: "canceled", failure: canceledCause, state: nexus.OperationStateCanceled},
		{name: "terminated", failure: terminatedCause, state: nexus.OperationStateFailed},
		{name: "timed out", failure: timedOutCause, state: nexus.OperationStateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := nexusrpc.CompleteOperationOptions{
				StartTime: startTime,
				CloseTime: closeTime,
				Links:     []nexus.Link{attachLink},
			}
			if tc.failure != nil {
				opts.Error = makeWrappedOpErr(t, tc.state, tc.failure)
			} else {
				opts.Result = tc.result
			}

			// 1. SystemCallbackURL (HTTP outbound path).
			var gotState nexus.OperationState
			var gotCloseTime time.Time
			var gotLinks []nexus.Link
			var gotPayload *commonpb.Payload
			var gotOpErr *nexus.OperationError
			httpHandler := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
				Serializer: commonnexus.PayloadSerializer,
				Handler: hsmCompletionHandlerFunc(func(_ context.Context, req *nexusrpc.CompletionRequest) error {
					gotState = req.State
					gotCloseTime = req.CloseTime
					gotLinks = req.Links
					if req.State == nexus.OperationStateSucceeded {
						return req.Result.Consume(&gotPayload)
					}
					gotOpErr = req.Error
					return nil
				}),
			})

			httpCB := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					Callback: &persistencespb.Callback{
						Variant: &persistencespb.Callback_Nexus_{
							Nexus: &persistencespb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
						},
						ResultSubscription: subMeta,
					},
					State:     enumsspb.CALLBACK_STATE_SCHEDULED,
					RequestId: "can-req-1",
				},
			}
			updated, err := executeHSMCallbackTask(t, &dualMutableState{subCompletion: opts}, "sub", httpCB, commonnexus.SystemCallbackURL, func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				httpHandler.ServeHTTP(rec, req)
				return rec.Result(), nil
			}, nil, log.NewTestLogger())
			require.NoError(t, err)
			require.Equal(t, enumsspb.CALLBACK_STATE_SUCCEEDED, updated.State())
			require.Equal(t, tc.state, gotState)
			require.Equal(t, closeTime, gotCloseTime.UTC())
			require.Len(t, gotLinks, 1)
			wantLinkEvent, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(attachLink)
			require.NoError(t, err)
			gotLinkEvent, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(gotLinks[0])
			require.NoError(t, err)
			protorequire.ProtoEqual(t, wantLinkEvent, gotLinkEvent)

			// 2. NexusCompletionHandlerURL (internal CHASM gRPC path).
			ctrl := gomock.NewController(t)
			var gotChasmReq *historyservice.CompleteNexusOperationChasmRequest
			historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
			historyClient.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *historyservice.CompleteNexusOperationChasmRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
					gotChasmReq = req
					return &historyservice.CompleteNexusOperationChasmResponse{}, nil
				},
			)
			internalCB := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					Callback: &persistencespb.Callback{
						Variant: &persistencespb.Callback_Nexus_{
							Nexus: &persistencespb.Callback_Nexus{
								Url:    chasm.NexusCompletionHandlerURL,
								Header: map[string]string{strings.ToLower(commonnexus.CallbackTokenHeader): encodedToken},
							},
						},
						ResultSubscription: subMeta,
					},
					State:     enumsspb.CALLBACK_STATE_SCHEDULED,
					RequestId: "can-req-1",
				},
			}
			updatedInt, err := executeHSMCallbackTask(t, &dualMutableState{subCompletion: opts}, "sub", internalCB, chasm.NexusCompletionHandlerURL, nil, historyClient, log.NewTestLogger())
			require.NoError(t, err)
			require.Equal(t, enumsspb.CALLBACK_STATE_SUCCEEDED, updatedInt.State())
			require.Equal(t, closeTime, gotChasmReq.GetCloseTime().AsTime().UTC())

			if tc.failure != nil {
				syncFailure, err := commonnexus.OperationErrorToTemporalFailure(opts.Error)
				require.NoError(t, err)
				protorequire.ProtoEqual(t, tc.failure, syncFailure)

				httpFailure, err := commonnexus.NexusFailureToTemporalFailure(*nexusrpc.UnwrapFailure(gotOpErr.OriginalFailure))
				require.NoError(t, err)
				protorequire.ProtoEqual(t, syncFailure, httpFailure)
				protorequire.ProtoEqual(t, syncFailure, gotChasmReq.GetFailure())
			} else {
				protorequire.ProtoEqual(t, tc.result, gotPayload)
				protorequire.ProtoEqual(t, tc.result, gotChasmReq.GetSuccess())
			}
		})
	}
}

func TestProcessInvocationTask_ResultSubscription_NonTerminalCloseIsPermanent(t *testing.T) {
	t.Parallel()

	subMeta := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: commoncallbacks.ResultSubscriptionFormatVersionV1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "origin-run"},
	}

	for _, tc := range []struct {
		name              string
		subErr            error
		wantUnprocessable bool
	}{
		{
			name:              "successor close event becomes permanent unprocessable error",
			subErr:            commoncallbacks.ErrResultSubscriptionNonTerminalClose,
			wantUnprocessable: true,
		},
		{
			name:              "transient read error stays retryable",
			subErr:            serviceerror.NewUnavailable("persistence unavailable"),
			wantUnprocessable: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cb := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					Callback: &persistencespb.Callback{
						Variant: &persistencespb.Callback_Nexus_{
							Nexus: &persistencespb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
						},
						ResultSubscription: subMeta,
					},
					State:     enumsspb.CALLBACK_STATE_SCHEDULED,
					RequestId: "attach-req-1",
				},
			}
			_, err := executeHSMCallbackTask(t, &dualMutableState{subErr: tc.subErr}, "sub", cb, commonnexus.SystemCallbackURL, nil, nil, log.NewTestLogger())
			require.ErrorIs(t, err, tc.subErr)
			var unprocessableErr *queueserrors.UnprocessableTaskError
			if tc.wantUnprocessable {
				require.ErrorAs(t, err, &unprocessableErr)
			} else {
				require.NotErrorAs(t, err, &unprocessableErr)
			}
		})
	}
}

type hsmCompletionHandlerFunc func(context.Context, *nexusrpc.CompletionRequest) error

func (f hsmCompletionHandlerFunc) CompleteOperation(ctx context.Context, req *nexusrpc.CompletionRequest) error {
	return f(ctx, req)
}
