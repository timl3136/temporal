package callback

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
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	commoncallbacks "go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/queues/common"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mockDualCompletionSource struct {
	chasm.UnimplementedComponent

	Empty *emptypb.Empty

	legacyCompletion nexusrpc.CompleteOperationOptions
	subCompletion    nexusrpc.CompleteOperationOptions
	subErr           error
	legacyCalls      []string
	subCalls         []struct {
		requestID string
		sub       *commonpb.Callback_ResultSubscription
	}

	Callbacks chasm.Map[string, *Callback]
}

var _ CompletionSource = (*mockDualCompletionSource)(nil)
var _ ResultSubscriptionCompletionSource = (*mockDualCompletionSource)(nil)

func (m *mockDualCompletionSource) GetNexusCompletion(_ chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	m.legacyCalls = append(m.legacyCalls, requestID)
	return m.legacyCompletion, nil
}

func (m *mockDualCompletionSource) GetResultSubscriptionCompletion(
	_ chasm.Context,
	requestID string,
	sub *commonpb.Callback_ResultSubscription,
) (nexusrpc.CompleteOperationOptions, error) {
	m.subCalls = append(m.subCalls, struct {
		requestID string
		sub       *commonpb.Callback_ResultSubscription
	}{requestID: requestID, sub: sub})
	return m.subCompletion, m.subErr
}

func (m *mockDualCompletionSource) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (m *mockDualCompletionSource) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

func (m *mockDualCompletionSource) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

type mockDualCompletionLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *mockDualCompletionLibrary) Name() string {
	return "mockdual"
}

func (l *mockDualCompletionLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*mockDualCompletionSource](
			"dualSource",
			chasm.WithExecutionType(enumspb.EXECUTION_TYPE_WORKFLOW),
		),
	}
}

func newDualCompletionTaskTest(
	t *testing.T,
	handler *invocationTaskHandler,
	source *mockDualCompletionSource,
	cbs map[string]*Callback,
) (context.Context, map[string]chasm.ComponentRef) {
	t.Helper()

	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, chasmRegistry.Register(&Library{InvocationTaskHandler: handler}))
	require.NoError(t, chasmRegistry.Register(&mockDualCompletionLibrary{}))

	executionKey := chasm.ExecutionKey{
		NamespaceID: "namespace-id",
		BusinessID:  "workflow-id",
		RunID:       "run-id",
	}
	engineCtx := chasm.NewEngineContext(t.Context(), chasmtest.NewEngine(t, chasmRegistry))
	_, err := chasm.StartExecution(
		engineCtx,
		executionKey,
		func(ctx chasm.MutableContext, _ struct{}) (*mockDualCompletionSource, error) {
			source.Callbacks = make(chasm.Map[string, *Callback], len(cbs))
			for k, cb := range cbs {
				source.Callbacks[k] = chasm.NewComponentField(ctx, cb)
			}
			return source, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	rootRef := chasm.NewComponentRef[*mockDualCompletionSource](executionKey)
	refs, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *mockDualCompletionSource, chasmCtx chasm.Context, _ struct{}) (map[string]chasm.ComponentRef, error) {
			out := make(map[string]chasm.ComponentRef, len(s.Callbacks))
			for k, field := range s.Callbacks {
				serialized, err := chasmCtx.Ref(field.Get(chasmCtx))
				if err != nil {
					return nil, err
				}
				ref, err := chasm.DeserializeComponentRef(serialized)
				if err != nil {
					return nil, err
				}
				out[k] = ref
			}
			return out, nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	return engineCtx, refs
}

func newTestInvocationHandler(
	t *testing.T,
	ns *namespace.Namespace,
	logger log.Logger,
	caller HTTPCaller,
	historyClient *historyservicemock.MockHistoryServiceClient,
) *invocationTaskHandler {
	t.Helper()
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	return newInvocationTaskHandler(invocationTaskHandlerOptions{
		Config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		NamespaceRegistry: nsRegistry,
		MetricsHandler:    metrics.NoopMetricsHandler,
		Logger:            logger,
		HTTPCallerProvider: func(common.NamespaceIDAndDestination) HTTPCaller {
			return caller
		},
		HistoryClient: historyClient,
	})
}

func TestLoadInvocationArgsResultSubscriptionValidation(t *testing.T) {
	t.Parallel()

	validSub := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: 1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run-1"},
	}

	cases := []struct {
		name              string
		cb                *callbackspb.Callback
		useLegacyOnlyHost bool
		wantErr           string
	}{
		{
			name: "format version 0 rejected",
			cb: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
				},
				ResultSubscription: &commonpb.Callback_ResultSubscription{
					RequestId:           "sub-req-1",
					ResultFormatVersion: 0,
				},
			},
			wantErr: "unsupported result subscription format version: 0",
		},
		{
			name: "format version 2 rejected",
			cb: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
				},
				ResultSubscription: &commonpb.Callback_ResultSubscription{
					RequestId:           "sub-req-1",
					ResultFormatVersion: 2,
				},
			},
			wantErr: "unsupported result subscription format version: 2",
		},
		{
			name: "missing subscription request ID rejected",
			cb: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
				},
				ResultSubscription: &commonpb.Callback_ResultSubscription{
					ResultFormatVersion: 1,
				},
			},
			wantErr: "result subscription callback missing request ID",
		},
		{
			name: "nexus handler variant rejected for result subscription",
			cb: &callbackspb.Callback{
				Variant: &callbackspb.Callback_NexusHandler_{
					NexusHandler: &callbackspb.Callback_NexusHandler{
						TaskQueueName: "tq",
						Service:       "svc",
						Operation:     "op",
					},
				},
				ResultSubscription: validSub,
			},
			wantErr: "unsupported callback variant for result subscription",
		},
		{
			name: "third-party URL rejected for result subscription",
			cb: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: "https://third-party.example/cb"},
				},
				ResultSubscription: validSub,
			},
			wantErr: "unsupported callback URL for result subscription",
		},
		{
			name: "completion source without result subscription support rejected",
			cb: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
				},
				ResultSubscription: validSub,
			},
			useLegacyOnlyHost: true,
			wantErr:           "does not support result subscriptions",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ns := test.NewNamespace(t)
			handler := newTestInvocationHandler(t, ns, log.NewTestLogger(), nil, nil)
			cb := &Callback{
				CallbackState: &callbackspb.CallbackState{
					RequestId: "attach-req-1",
					Callback:  tc.cb,
					Status:    callbackspb.CALLBACK_STATUS_SCHEDULED,
				},
			}

			var engineCtx context.Context
			var cbRef chasm.ComponentRef
			if tc.useLegacyOnlyHost {
				engineCtx, cbRef = newInvocationTaskTest(t, handler, cb, nexusrpc.CompleteOperationOptions{})
			} else {
				var refs map[string]chasm.ComponentRef
				engineCtx, refs = newDualCompletionTaskTest(t, handler, &mockDualCompletionSource{}, map[string]*Callback{"sub": cb})
				cbRef = refs["sub"]
			}

			err := handler.Execute(
				engineCtx,
				cbRef,
				chasm.TaskAttributes{Destination: commonnexus.SystemCallbackURL},
				&callbackspb.InvocationTask{Attempt: 0},
			)
			var unprocessableErr *queueserrors.UnprocessableTaskError
			require.ErrorAs(t, err, &unprocessableErr)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestExecuteInvocationTask_ResultSubscriptionAndLegacySameTarget(t *testing.T) {
	t.Parallel()

	ns := test.NewNamespace(t)
	subPayload := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/encrypted")},
		Data:     []byte{0xff, 0x00, 0x80},
	}
	legacyPayload := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(`"legacy"`),
	}

	received := make(map[string]*commonpb.Payload)
	handler := newTestInvocationHandler(t, ns, log.NewTestLogger(), func(req *http.Request) (*http.Response, error) {
		var p *commonpb.Payload
		h := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
			Serializer: commonnexus.PayloadSerializer,
			Handler: completionHandlerFunc(func(_ context.Context, comp *nexusrpc.CompletionRequest) error {
				return comp.Result.Consume(&p)
			}),
		})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		received[req.URL.String()] = p
		return rec.Result(), nil
	}, nil)

	subMeta := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: 1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "origin-run"},
	}
	source := &mockDualCompletionSource{
		legacyCompletion: nexusrpc.CompleteOperationOptions{Result: legacyPayload},
		subCompletion:    nexusrpc.CompleteOperationOptions{Result: subPayload},
	}
	subCB := &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId: "can-req-1",
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
				},
				ResultSubscription: subMeta,
			},
			Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
		},
	}
	legacyCB := &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId: "legacy-req-1",
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{Url: "http://third-party.example/cb"},
				},
			},
			Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
		},
	}

	engineCtx, refs := newDualCompletionTaskTest(t, handler, source, map[string]*Callback{
		"sub":    subCB,
		"legacy": legacyCB,
	})

	require.NoError(t, handler.Execute(
		engineCtx,
		refs["sub"],
		chasm.TaskAttributes{Destination: commonnexus.SystemCallbackURL},
		&callbackspb.InvocationTask{Attempt: 0},
	))
	require.NoError(t, handler.Execute(
		engineCtx,
		refs["legacy"],
		chasm.TaskAttributes{Destination: "http://third-party.example"},
		&callbackspb.InvocationTask{Attempt: 0},
	))

	require.Len(t, source.subCalls, 1)
	require.Equal(t, "can-req-1", source.subCalls[0].requestID)
	protorequire.ProtoEqual(t, subMeta, source.subCalls[0].sub)
	require.Equal(t, []string{"legacy-req-1"}, source.legacyCalls)

	protorequire.ProtoEqual(t, subPayload, received[commonnexus.SystemCallbackURL])
	protorequire.ProtoEqual(t, legacyPayload, received["http://third-party.example/cb"])
}

func TestExecuteInvocationTask_ResultSubscription_RetryAndPermanentErrors(t *testing.T) {
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
			wantStatus  callbackspb.CallbackStatus
			wantDestErr bool
		}{
			{name: "success", statusCode: http.StatusOK, wantStatus: callbackspb.CALLBACK_STATUS_SUCCEEDED},
			{name: "retryable 500 backs off", statusCode: http.StatusInternalServerError, wantStatus: callbackspb.CALLBACK_STATUS_BACKING_OFF, wantDestErr: true},
			{name: "permanent 404 marks failed", statusCode: http.StatusNotFound, wantStatus: callbackspb.CALLBACK_STATUS_FAILED},
			{name: "permanent 400 marks failed", statusCode: http.StatusBadRequest, wantStatus: callbackspb.CALLBACK_STATUS_FAILED},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ns := test.NewNamespace(t)
				logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
				capture := logger.StartCapture()
				handler := newTestInvocationHandler(t, ns, logger, func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: tc.statusCode, Body: http.NoBody}, nil
				}, nil)

				cb := &Callback{
					CallbackState: &callbackspb.CallbackState{
						RequestId: "can-req-1",
						Callback: &callbackspb.Callback{
							Variant: &callbackspb.Callback_Nexus_{
								Nexus: &callbackspb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
							},
							ResultSubscription: subMeta,
						},
						Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
					},
				}
				engineCtx, refs := newDualCompletionTaskTest(t, handler, &mockDualCompletionSource{}, map[string]*Callback{"sub": cb})
				err := handler.Execute(
					engineCtx,
					refs["sub"],
					chasm.TaskAttributes{Destination: commonnexus.SystemCallbackURL},
					&callbackspb.InvocationTask{Attempt: 0},
				)
				if tc.wantDestErr {
					var destDownErr *queueserrors.DestinationDownError
					require.ErrorAs(t, err, &destDownErr)
				} else {
					require.NoError(t, err)
				}
				readCallbackState(engineCtx, t, refs["sub"], func(_ chasm.Context, got *Callback) {
					require.Equal(t, tc.wantStatus, got.Status)
				})
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
		// Raw ref token (no embedded request ID) exercises fallback to sub.RequestId over CAN.
		encodedToken := base64.RawURLEncoding.EncodeToString(refBytes)

		for _, tc := range []struct {
			name       string
			rpcErr     error
			wantStatus callbackspb.CallbackStatus
		}{
			{name: "success", wantStatus: callbackspb.CALLBACK_STATUS_SUCCEEDED},
			{name: "retryable unavailable backs off", rpcErr: serviceerror.NewUnavailable("unavailable"), wantStatus: callbackspb.CALLBACK_STATUS_BACKING_OFF},
			{name: "permanent not found marks failed", rpcErr: serviceerror.NewNotFound("gone"), wantStatus: callbackspb.CALLBACK_STATUS_FAILED},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ns := test.NewNamespace(t)
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
				handler := newTestInvocationHandler(t, ns, log.NewTestLogger(), nil, historyClient)

				cb := &Callback{
					CallbackState: &callbackspb.CallbackState{
						RequestId: "can-req-1",
						Callback: &callbackspb.Callback{
							Variant: &callbackspb.Callback_Nexus_{
								Nexus: &callbackspb.Callback_Nexus{
									Url:    chasm.NexusCompletionHandlerURL,
									Header: map[string]string{strings.ToLower(commonnexus.CallbackTokenHeader): encodedToken},
								},
							},
							ResultSubscription: subMeta,
						},
						Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
					},
				}
				engineCtx, refs := newDualCompletionTaskTest(t, handler, &mockDualCompletionSource{}, map[string]*Callback{"sub": cb})
				err := handler.Execute(
					engineCtx,
					refs["sub"],
					chasm.TaskAttributes{Destination: chasm.NexusCompletionHandlerURL},
					&callbackspb.InvocationTask{Attempt: 0},
				)
				if tc.rpcErr != nil {
					require.ErrorContains(t, err, "internal error, reference-id:")
				} else {
					require.NoError(t, err)
				}
				readCallbackState(engineCtx, t, refs["sub"], func(_ chasm.Context, got *Callback) {
					require.Equal(t, tc.wantStatus, got.Status)
				})
			})
		}
	})
}

func TestExecuteInvocationTask_ResultSubscription_WireParity(t *testing.T) {
	t.Parallel()

	stored := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/encrypted"), "codec": {0x80, 0x00, 0xff}},
		Data:     []byte{0xff, 0x00, 0x80},
	}
	details := &commonpb.Payloads{Payloads: []*commonpb.Payload{stored}}
	closeTime := time.Date(2026, 11, 25, 12, 30, 0, 0, time.UTC)
	startTime := closeTime.Add(-time.Minute)
	attachLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
		Namespace:  "ns",
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

			// 1. Deliver via SystemCallbackURL (HTTP outbound path).
			var gotState nexus.OperationState
			var gotCloseTime time.Time
			var gotLinks []nexus.Link
			var gotPayload *commonpb.Payload
			var gotOpErr *nexus.OperationError
			httpHandler := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
				Serializer: commonnexus.PayloadSerializer,
				Handler: completionHandlerFunc(func(_ context.Context, req *nexusrpc.CompletionRequest) error {
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

			ns := test.NewNamespace(t)
			handler := newTestInvocationHandler(t, ns, log.NewTestLogger(), func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				httpHandler.ServeHTTP(rec, req)
				return rec.Result(), nil
			}, nil)
			httpCB := &Callback{
				CallbackState: &callbackspb.CallbackState{
					RequestId: "can-req-1",
					Callback: &callbackspb.Callback{
						Variant: &callbackspb.Callback_Nexus_{
							Nexus: &callbackspb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
						},
						ResultSubscription: subMeta,
					},
					Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
				},
			}
			engineCtx, refs := newDualCompletionTaskTest(t, handler, &mockDualCompletionSource{subCompletion: opts}, map[string]*Callback{"sub": httpCB})
			require.NoError(t, handler.Execute(
				engineCtx,
				refs["sub"],
				chasm.TaskAttributes{Destination: commonnexus.SystemCallbackURL},
				&callbackspb.InvocationTask{Attempt: 0},
			))

			require.Equal(t, tc.state, gotState)
			require.Equal(t, closeTime, gotCloseTime.UTC())
			require.Len(t, gotLinks, 1)
			wantLinkEvent, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(attachLink)
			require.NoError(t, err)
			gotLinkEvent, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(gotLinks[0])
			require.NoError(t, err)
			protorequire.ProtoEqual(t, wantLinkEvent, gotLinkEvent)

			// 2. Deliver via NexusCompletionHandlerURL (internal CHASM gRPC path).
			ctrl := gomock.NewController(t)
			var gotChasmReq *historyservice.CompleteNexusOperationChasmRequest
			historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
			historyClient.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *historyservice.CompleteNexusOperationChasmRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
					gotChasmReq = req
					return &historyservice.CompleteNexusOperationChasmResponse{}, nil
				},
			)
			internalHandler := newTestInvocationHandler(t, ns, log.NewTestLogger(), nil, historyClient)
			internalCB := &Callback{
				CallbackState: &callbackspb.CallbackState{
					RequestId: "can-req-1",
					Callback: &callbackspb.Callback{
						Variant: &callbackspb.Callback_Nexus_{
							Nexus: &callbackspb.Callback_Nexus{
								Url:    chasm.NexusCompletionHandlerURL,
								Header: map[string]string{strings.ToLower(commonnexus.CallbackTokenHeader): encodedToken},
							},
						},
						ResultSubscription: subMeta,
					},
					Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
				},
			}
			intCtx, intRefs := newDualCompletionTaskTest(t, internalHandler, &mockDualCompletionSource{subCompletion: opts}, map[string]*Callback{"sub": internalCB})
			require.NoError(t, internalHandler.Execute(
				intCtx,
				intRefs["sub"],
				chasm.TaskAttributes{Destination: chasm.NexusCompletionHandlerURL},
				&callbackspb.InvocationTask{Attempt: 0},
			))
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

func TestExecuteInvocationTask_ResultSubscription_NonTerminalCloseIsPermanent(t *testing.T) {
	t.Parallel()

	subMeta := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: commoncallbacks.ResultSubscriptionFormatVersionV1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "origin-run"},
	}

	newCB := func() *Callback {
		return &Callback{
			CallbackState: &callbackspb.CallbackState{
				RequestId: "attach-req-1",
				Callback: &callbackspb.Callback{
					Variant: &callbackspb.Callback_Nexus_{
						Nexus: &callbackspb.Callback_Nexus{Url: commonnexus.SystemCallbackURL},
					},
					ResultSubscription: subMeta,
				},
				Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
			},
		}
	}

	t.Run("successor close event fails callback permanently", func(t *testing.T) {
		t.Parallel()

		ns := test.NewNamespace(t)
		handler := newTestInvocationHandler(t, ns, log.NewTestLogger(), nil, nil)
		engineCtx, refs := newDualCompletionTaskTest(
			t,
			handler,
			&mockDualCompletionSource{subErr: commoncallbacks.ErrResultSubscriptionNonTerminalClose},
			map[string]*Callback{"sub": newCB()},
		)
		err := handler.Execute(
			engineCtx,
			refs["sub"],
			chasm.TaskAttributes{Destination: commonnexus.SystemCallbackURL},
			&callbackspb.InvocationTask{Attempt: 0},
		)
		require.NoError(t, err)
		readCallbackState(engineCtx, t, refs["sub"], func(_ chasm.Context, got *Callback) {
			require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, got.Status)
			require.Contains(t, got.GetLastAttemptFailure().GetMessage(), commoncallbacks.ErrResultSubscriptionNonTerminalClose.Error())
			require.True(t, got.GetLastAttemptFailure().GetApplicationFailureInfo().GetNonRetryable())
		})
	})

	t.Run("transient read error stays retryable", func(t *testing.T) {
		t.Parallel()

		transientErr := serviceerror.NewUnavailable("persistence unavailable")
		ns := test.NewNamespace(t)
		handler := newTestInvocationHandler(t, ns, log.NewTestLogger(), nil, nil)
		engineCtx, refs := newDualCompletionTaskTest(
			t,
			handler,
			&mockDualCompletionSource{subErr: transientErr},
			map[string]*Callback{"sub": newCB()},
		)
		err := handler.Execute(
			engineCtx,
			refs["sub"],
			chasm.TaskAttributes{Destination: commonnexus.SystemCallbackURL},
			&callbackspb.InvocationTask{Attempt: 0},
		)
		require.ErrorIs(t, err, transientErr)
		var unprocessableErr *queueserrors.UnprocessableTaskError
		require.NotErrorAs(t, err, &unprocessableErr)
		readCallbackState(engineCtx, t, refs["sub"], func(_ chasm.Context, got *Callback) {
			require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, got.Status)
		})
	})
}

type completionHandlerFunc func(context.Context, *nexusrpc.CompletionRequest) error

func (f completionHandlerFunc) CompleteOperation(ctx context.Context, req *nexusrpc.CompletionRequest) error {
	return f(ctx, req)
}
