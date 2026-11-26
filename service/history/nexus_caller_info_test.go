package history_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"google.golang.org/protobuf/proto"
)

func TestNexusCallerInfoIsOptional(t *testing.T) {
	cases := []struct {
		name       string
		newRequest func() proto.Message
		callerInfo func(proto.Message) *historyservice.NexusCallerInfo
	}{
		{
			name: "start",
			newRequest: func() proto.Message {
				return &historyservice.StartNexusOperationRequest{NamespaceId: "namespace-id"}
			},
			callerInfo: func(msg proto.Message) *historyservice.NexusCallerInfo {
				return msg.(*historyservice.StartNexusOperationRequest).GetCallerInfo()
			},
		},
		{
			name: "cancel",
			newRequest: func() proto.Message {
				return &historyservice.CancelNexusOperationRequest{NamespaceId: "namespace-id"}
			},
			callerInfo: func(msg proto.Message) *historyservice.NexusCallerInfo {
				return msg.(*historyservice.CancelNexusOperationRequest).GetCallerInfo()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := proto.Marshal(tc.newRequest())
			require.NoError(t, err)
			decoded := tc.newRequest()
			require.NoError(t, proto.Unmarshal(wire, decoded))
			require.Nil(t, tc.callerInfo(decoded))
		})
	}
}
