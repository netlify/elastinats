package messaging

import "time"

const (
	rawMsgKey    = "@raw_msg"
	timestampKey = "@timestamp"
	sourceKey    = "@source"
)

type Payload map[string]interface{}

func NewPayload(msg, source string) *Payload {
	return &Payload{
		rawMsgKey:    msg,
		sourceKey:    source,
		timestampKey: time.Now().Format(time.RFC3339),
	}
}
