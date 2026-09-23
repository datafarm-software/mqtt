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

type Exception struct {
	error
	Topic string
}

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
	wg         sync.WaitGroup
	client     mqtt.Client
	processors map[string]*processor
	exceptions chan Exception
}

func NewHandler(opts Opts) (MqttHandler, error) {
	h := &handler{
		processors: make(map[string]*processor),
		opts:       opts,
		exceptions: make(chan Exception, 10),
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
			log.Printf("Mqtt Connector - unsubscribing from topic: %s", topic)
		}
		if err = processor.close(); err != nil {
			log.Printf("Mqtt Connector - closing processor for: %s, error: %v", topic, err)
		}
	}
	close(h.exceptions)
	h.client.Disconnect(250)
	return nil
}

func (h *handler) GetClient() (mqtt.Client, error) {
	if !h.client.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}
	return h.client, nil
}

func (h *handler) GetExceptions() <-chan Exception {
	return h.exceptions
}

func (h *handler) AsyncProcess(ctx context.Context, topic string, numWorkers int,
	pf ProcessFunc) error {
	token := h.client.Subscribe(topic, 1, nil)
	if ok := token.WaitTimeout(100 * time.Millisecond); !ok {
		return fmt.Errorf("timeout subscribing to topic: %s", topic)
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &processor{
		ctx:            ctx,
		cancel:         cancel,
		numWorkers:     numWorkers,
		processFunc:    pf,
		payloadChannel: make(chan []byte, 1024),
		exceptionsChan: h.exceptions,
		topic:          topic,
	}
	h.processors[topic] = p
	p.asyncProcess()
	log.Printf("Mqtt Connector - Processing Topic: %s", topic)
	return nil
}

func (h *handler) MessageHandler(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	if p, ok := h.processors[topic]; ok {
		p.sendPayload(msg.Payload())
		return
	}
	for wildcard := range h.processors {
		if h.match(wildcard, topic) {
			if p, ok := h.processors[wildcard]; ok {
				p.sendPayload(msg.Payload())
				return
			}
		}
	}
}

func (h *handler) connectHandler(client mqtt.Client) {
	log.Println("Mqtt Connector - Client Connected")
	var err error
	for topic, p := range h.processors {
		err = h.AsyncProcess(p.ctx, topic, p.numWorkers, p.processFunc)
		if err != nil {
			log.Printf("Mqtt Connector - while re subscribing to %s, error: %v", topic, err)
		}
	}
}

func (h *handler) connectLostHandler(client mqtt.Client, err error) {
	log.Printf("Mqtt Connector - Connection lost: %v", err)
	log.Printf("Mqtt Connector - Reconnecting")
	var connectSuccess bool
	for i := range 59 {
		time.Sleep(1 * time.Minute)
		if err := h.mqttClient(); err != nil {
			log.Printf("Mqtt Connector - on reconnect attempt %d: %v", i+1, err)
		} else {
			connectSuccess = true
		}
		if connectSuccess {
			break
		}
	}
	log.Printf("Mqtt Connector - Client Reconnected")
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
	topic          string
	cancel         context.CancelFunc
	numWorkers     int
	processFunc    ProcessFunc
	payloadChannel chan []byte
	exceptionsChan chan<- Exception
}

func (p *processor) close() error {
	p.cancel()
	p.wg.Wait()
	p.once.Do(func() {
		close(p.payloadChannel)
	})
	return nil
}

func (p *processor) sendPayload(payload []byte) {
	p.payloadChannel <- payload
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
					ticker := time.NewTicker(100 * time.Millisecond)
					defer ticker.Stop()
					select {
					case p.exceptionsChan <- Exception{err, p.topic}:
					case <-ticker.C:
						return
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
