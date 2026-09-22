package mqtt_connector

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type ProcessFunc func([]byte) error

type MqttHandler interface {
	Close() error
	GetClient() (mqtt.Client, error)
	AsyncProcess(ctx context.Context, topic string, numWorkers int,
		processFunc ProcessFunc) error
	MessageHandler(client mqtt.Client, msg mqtt.Message)
}

type Opts struct {
	Broker   string `mapstructure:"broker" validate:"required"`
	Port     int    `mapstructure:"port" validate:"required"`
	ClientId string `mapstructure:"clientid" validate:"required"`
	Username string `mapstructure:"username" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
}

func ConnectMqtt(opts Opts) (mqtt.Client, error) {
	o := mqtt.NewClientOptions()
	o.AddBroker(fmt.Sprintf("tcp://%s:%d", opts.Broker, opts.Port))
	o.SetClientID(opts.ClientId)
	o.SetUsername(opts.Username)
	o.SetPassword(opts.Password)
	client := mqtt.NewClient(o)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("Error connecting to MQTT: %v", token.Error())
	}
	return client, nil
}

type handler struct {
	opts       Opts
	client     mqtt.Client
	processors map[string]*processor
}

func NewHandler(opts Opts) (MqttHandler, error) {
	h := &handler{
		processors: make(map[string]*processor),
		opts:       opts,
	}
	err := h.mqttClient()
	return h, err
}

func (h *handler) mqttClient() error {
	o := mqtt.NewClientOptions()
	o.AddBroker(fmt.Sprintf("tcp://%s:%d", h.opts.Broker, h.opts.Port))
	o.SetClientID(h.opts.ClientId)
	o.SetUsername(h.opts.Username)
	o.SetPassword(h.opts.Password)
	o.SetDefaultPublishHandler(h.MessageHandler)
	o.OnConnect = h.connectHandler
	o.OnConnectionLost = h.connectLostHandler
	client := mqtt.NewClient(o)
	token := client.Connect()
	if !token.WaitTimeout(100 * time.Millisecond) {
		return fmt.Errorf("Timeout connecting.")
	}
	if token.Error() != nil {
		return fmt.Errorf("Connecting to MQTT: %v", token.Error())
	}
	h.client = client
	return nil
}

func (h *handler) Close() (err error) {
	for topic, processor := range h.processors {
		if token := h.client.Unsubscribe(topic); !token.WaitTimeout(100 * time.Millisecond) {
			log.Printf("unsubscribing from topic: %s", topic)
		}
		if err = processor.Close(); err != nil {
			log.Printf("closing processor for: %s, error: %v", topic, err)
		}
	}
	h.client.Disconnect(250)
	return nil
}

func (h *handler) GetClient() (mqtt.Client, error) {
	if !h.client.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}
	return h.client, nil
}

func (h *handler) AsyncProcess(ctx context.Context, topic string, numWorkers int,
	pf ProcessFunc) error {
	token := h.client.Subscribe(topic, 1, nil)
	if ok := token.WaitTimeout(100 * time.Millisecond); !ok {
		return fmt.Errorf("timeout subscribing to topic: %s", topic)
	}
	p := &processor{
		ctx:            ctx,
		numWorkers:     numWorkers,
		processFunc:    pf,
		payloadChannel: make(chan []byte, 1024),
		errorChannel:   make(chan error, 10),
	}
	h.processors[topic] = p
	p.asyncProcess()
	log.Printf("Mqtt Connector - Processing Topic: %s", topic)
	return nil
}

func (h *handler) MessageHandler(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	if p, ok := h.processors[topic]; ok {
		p.SendPayload(msg.Payload())
		return
	}
	for wildcard := range h.processors {
		if h.match(wildcard, topic) {
			if p, ok := h.processors[wildcard]; ok {
				p.SendPayload(msg.Payload())
				return
			}
		}
	}
}

func (h *handler) connectHandler(client mqtt.Client) {
	log.Println("Mqtt Connector - Client Connected")
}

func (h *handler) connectLostHandler(client mqtt.Client, err error) {
	log.Printf("Mqtt Connector - Connection lost: %v", err)
	log.Printf("Reconnecting")
	var connectSuccess, subscribeSuccess bool
	for i := range 59 {
		time.Sleep(1 * time.Minute)
		if !connectSuccess {
			if err := h.mqttClient(); err != nil {
				log.Printf("on reconnect attempt %d: %v", i+1, err)
				continue
			}
			connectSuccess = true
		}
		if !subscribeSuccess {
		innerLoop:
			for topic, p := range h.processors {
				err = h.AsyncProcess(p.ctx, topic, p.numWorkers, p.processFunc)
				if err != nil {
					log.Printf("re subscribing to %s, error: %v", topic, err)
					continue innerLoop
				}
				h.processors[topic] = p
			}
			subscribeSuccess = true
		}
		if connectSuccess && subscribeSuccess {
			break
		}
	}
	log.Printf("MQTT Connector - Reconnected and Resubscribed")
}

func (h *handler) match(wildcard, topic string) bool {
	if wildcard == topic {
		return true
	}
	wildcardParts := strings.Split(wildcard, "/")
	topicParts := strings.Split(topic, "/")
	if len(wildcardParts) != len(topicParts) {
		return false
	}
	for i, wildcardPart := range wildcardParts {
		if wildcardPart == "+" {
			continue
		}
		if wildcardPart == topicParts[i] {
			continue
		}
		return false
	}
	return true
}

type processor struct {
	ctx            context.Context
	wg             sync.WaitGroup
	once           sync.Once
	numWorkers     int
	processFunc    ProcessFunc
	payloadChannel chan []byte
	errorChannel   chan error
}

func (p *processor) Close() error {
	p.once.Do(func() {
		close(p.payloadChannel)
		close(p.errorChannel)
	})
	return nil
}

func (p *processor) SendPayload(payload []byte) {
	p.payloadChannel <- payload
}

func (p *processor) GetErrorChannel() chan error {
	return p.errorChannel
}

func (p *processor) asyncProcess() {
	workerTask := func() {
		for {
			select {
			case payload, ok := <-p.payloadChannel:
				if !ok {
					return
				}
				if err := p.processFunc(payload); err != nil {
					select {
					case p.GetErrorChannel() <- err:
					case <-p.ctx.Done():
						return
					}
				}
			case <-p.ctx.Done():
				return
			}
		}
	}
	if p.numWorkers < 1 {
		p.numWorkers = 1
	}
	for range p.numWorkers {
		p.wg.Go(workerTask)
	}
}
