package mqtt

import (
	"context"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

var dh = &handler{
	client:     &MockMqttClient{},
	processors: make(map[string]TopicProcessor),
}

type MockToken struct{}

func (t *MockToken) Wait() bool {
	return true
}
func (t *MockToken) WaitTimeout(time.Duration) bool {
	return true
}
func (t *MockToken) Done() <-chan struct{} {
	return make(<-chan struct{})
}
func (t *MockToken) Error() error {
	return nil
}

type MockMqttClient struct{}

func (m *MockMqttClient) IsConnected() bool {
	return true
}
func (m *MockMqttClient) IsConnectionOpen() bool {
	return true
}
func (m *MockMqttClient) Connect() pahomqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) Disconnect(quiesce uint) {}
func (m *MockMqttClient) Publish(
	topic string,
	qos byte,
	retained bool,
	payload interface{},
) pahomqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) Subscribe(
	topic string,
	qos byte,
	callBack pahomqtt.MessageHandler,
) pahomqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) SubscribeMultiple(
	filters map[string]byte,
	callback pahomqtt.MessageHandler,
) pahomqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) Unsubscribe(topics ...string) pahomqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) AddRoute(topic string, callback pahomqtt.MessageHandler) {}
func (m *MockMqttClient) OptionsReader() pahomqtt.ClientOptionsReader {
	return pahomqtt.ClientOptionsReader{}
}

func TestSubscribe(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{
			name:  "Successful Subscribe",
			topic: "success",
		},
	}

	for _, tt := range tests {
		_, err := dh.Subscribe(tt.topic)
		if tt.wantErr != (err != nil) {
			t.Fatalf("%s FAILED: expected err: %v, got: %v", tt.name, tt.wantErr, err)
		}
		if _, ok := dh.processors[tt.topic]; !ok {
			t.Fatalf("%s channel is invalid", tt.topic)
		}
	}
}

func TestPayloadProcess(t *testing.T) {
	p := &processor{}
	processFunc := func(_ []byte) error {
		return nil
	}
	tests := []struct {
		name      string
		cancelCtx bool
		wantErr   bool
	}{
		{
			name:      "returns after receiving payload",
			cancelCtx: false,
			wantErr:   false,
		},
		{
			name:      "returns after context cancelled",
			cancelCtx: true,
			wantErr:   true,
		},
	}
	var wg sync.WaitGroup
	for _, tt := range tests {
		ctx, cancel := context.WithCancel(context.Background())
		p.payloadChannel = make(chan []byte)
		defer cancel()
		defer close(p.payloadChannel)
		p.errorChannel = make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.PayloadProcess(ctx, processFunc)
			p.errorChannel <- err
		}()
		if tt.cancelCtx {
			cancel()
		} else {
			p.payloadChannel <- []byte("test data")
		}
		wg.Wait()
		close(p.errorChannel)
		for err := range p.errorChannel {
			if tt.wantErr != (err != nil) {
				t.Fatalf("%s FAILED: expected error: %v, got: %v", tt.name, tt.wantErr, err)
			}
		}
	}
}

func TestMatch(t *testing.T) {
	tests := []struct {
		name     string
		wildcard string
		topic    string
		want     bool
	}{
		{
			name:     "match identical",
			wildcard: "identical",
			topic:    "identical",
			want:     true,
		},
		{
			name:     "match wildcard",
			wildcard: "match/+/wildcard",
			topic:    "match/some/wildcard",
			want:     true,
		},
		{
			name:     "no match",
			wildcard: "no",
			topic:    "match",
			want:     false,
		},
		{
			name:     "no match wildcard",
			wildcard: "no/+",
			topic:    "no/match/wildcard",
			want:     false,
		},
		{
			name:     "no match different part lengths",
			wildcard: "part/1",
			topic:    "part/1/2",
			want:     false,
		},
	}

	for _, tt := range tests {
		if ok := dh.match(tt.wildcard, tt.topic); ok != tt.want {
			t.Fatalf("%s FAILED expected %v, got %v", tt.name, tt.want, ok)
		}
	}
}

type MockMqttMessage struct {
	topic string
}

func newMockMqttMessage(topic string) *MockMqttMessage {
	return &MockMqttMessage{
		topic: topic,
	}
}

func (m *MockMqttMessage) Duplicate() bool   { return false }
func (m *MockMqttMessage) Qos() byte         { return byte(1) }
func (m *MockMqttMessage) Retained() bool    { return false }
func (m *MockMqttMessage) Topic() string     { return m.topic }
func (m *MockMqttMessage) MessageID() uint16 { return uint16(1) }
func (m *MockMqttMessage) Payload() []byte   { return nil }
func (m *MockMqttMessage) Ack()              {}

type MockProcessor struct {
}

func (m *MockProcessor) Close() error                { return nil }
func (m *MockProcessor) SendPayload(payload []byte)  { payloadSent++ }
func (m *MockProcessor) GetErrorChannel() chan error { return nil }
func (m *MockProcessor) AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc ProcessFunc) {
}
func (m *MockProcessor) PayloadProcess(ctx context.Context, processFunc ProcessFunc) error {
	return nil
}

var payloadSent int

func TestMessageHandler(t *testing.T) {
	tests := []struct {
		name           string
		subscribeTopic string
		messageTopic   string
		wantSendCount  int
	}{
		{
			name:           "successful send",
			subscribeTopic: "someTopic",
			messageTopic:   "someTopic",
			wantSendCount:  1,
		},
		{
			name:           "successful wildcard send",
			subscribeTopic: "some/+/wildcard",
			messageTopic:   "some/successful/wildcard",
			wantSendCount:  1,
		},
		{
			name:           "unsuccessful send",
			subscribeTopic: "something",
			messageTopic:   "else",
			wantSendCount:  0,
		},
	}
	for _, tt := range tests {
		payloadSent = 0
		message := newMockMqttMessage(tt.messageTopic)
		h := &handler{
			processors: map[string]TopicProcessor{
				tt.subscribeTopic: &MockProcessor{},
			},
		}
		h.MessageHandler(&MockMqttClient{}, message)
		if payloadSent != tt.wantSendCount {
			t.Fatalf("payload sent not equal to 1. Rather: %d", payloadSent)
		}
	}
}
