package callbacks

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	commonnexus "go.temporal.io/server/common/nexus"
)

func TestValidateResultSubscriptionForDelivery(t *testing.T) {
	t.Parallel()

	validSub := &commonpb.Callback_ResultSubscription{
		RequestId:           "sub-req-1",
		ResultFormatVersion: ResultSubscriptionFormatVersionV1,
		ResolvedExecution:   &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run-1"},
	}

	for _, tc := range []struct {
		name    string
		url     string
		sub     *commonpb.Callback_ResultSubscription
		wantErr string
	}{
		{
			name: "valid system callback URL",
			url:  commonnexus.SystemCallbackURL,
			sub:  validSub,
		},
		{
			name: "valid internal callback URL",
			url:  chasm.NexusCompletionHandlerURL,
			sub:  validSub,
		},
		{
			name:    "nil subscription",
			url:     commonnexus.SystemCallbackURL,
			wantErr: "unsupported result subscription format version: 0",
		},
		{
			name: "version 0 rejected",
			url:  commonnexus.SystemCallbackURL,
			sub: &commonpb.Callback_ResultSubscription{
				RequestId:           "sub-req-1",
				ResultFormatVersion: 0,
			},
			wantErr: "unsupported result subscription format version: 0",
		},
		{
			name: "version 2 rejected",
			url:  commonnexus.SystemCallbackURL,
			sub: &commonpb.Callback_ResultSubscription{
				RequestId:           "sub-req-1",
				ResultFormatVersion: 2,
			},
			wantErr: "unsupported result subscription format version: 2",
		},
		{
			name: "missing request ID rejected",
			url:  commonnexus.SystemCallbackURL,
			sub: &commonpb.Callback_ResultSubscription{
				ResultFormatVersion: ResultSubscriptionFormatVersionV1,
			},
			wantErr: "result subscription callback missing request ID",
		},
		{
			name:    "external URL rejected",
			url:     "https://third-party.example/cb",
			sub:     validSub,
			wantErr: "unsupported callback URL for result subscription",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateResultSubscriptionForDelivery(tc.url, tc.sub)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
