package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/kelseyhightower/envconfig"
)

// KNMQTTAdapterSettings mirrors the configuration from the Python code.
type KNMQTTAdapterSettings struct {
	Host                   string `envconfig:"KN_MQTT_HOST" default:"0.0.0.0"`
	Port                   int    `envconfig:"KN_MQTT_PORT" default:"8080"`
	MQTTBroker             string `envconfig:"KN_MQTT_MQTT_BROKER" default:"mosquitto.default"`
	MQTTPort               int    `envconfig:"KN_MQTT_MQTT_PORT" default:"1883"`
	MQTTTopicSubscriptions string `envconfig:"KN_MQTT_MQTT_TOPIC_SUBSCRIPTIONS" default:"envds/+/+/+/data/#"`
	KnativeBroker          string `envconfig:"KN_MQTT_KNATIVE_BROKER" default:"http://broker-ingress.knative-eventing.svc.cluster.local/default/default"`
	ValidationRequired     bool   `envconfig:"KN_MQTT_VALIDATION_REQUIRED" default:"false"`
}

// KNMQTTClient encapsulates the core logic.
type KNMQTTClient struct {
	config           KNMQTTAdapterSettings
	mqttClient       mqtt.Client
	toMqttChannel    chan event.Event
	toKnativeChannel chan event.Event
}

// NewKNMQTTClient initializes a new client.
func NewKNMQTTClient(config KNMQTTAdapterSettings) *KNMQTTClient {
	c := &KNMQTTClient{
		config:           config,
		toMqttChannel:    make(chan event.Event, 100),
		toKnativeChannel: make(chan event.Event, 100),
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", config.MQTTBroker, config.MQTTPort))
	opts.SetClientID(fmt.Sprintf("kn-mqtt-adapter-%d", time.Now().UnixNano()))
	opts.SetDefaultPublishHandler(c.mqttMessageHandler)

	c.mqttClient = mqtt.NewClient(opts)
	if token := c.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to MQTT broker: %v", token.Error())
	}

	c.subscribeToMqttTopics()

	go c.sendToMqttLoop()
	go c.sendToKnativeLoop()

	return c
}

// subscribeToMqttTopics subscribes to all configured topics.
func (c *KNMQTTClient) subscribeToMqttTopics() {
	topics := strings.Split(c.config.MQTTTopicSubscriptions, ",")
	for _, topic := range topics {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			continue
		}
		// The python code uses a shared subscription, so we will replicate that here.
		fullTopic := fmt.Sprintf("$share/knative/%s", strings.TrimSpace(topic))
		if token := c.mqttClient.Subscribe(fullTopic, 1, nil); token.Wait() && token.Error() != nil {
			log.Printf("Error subscribing to topic %s: %v", fullTopic, token.Error())
		} else {
			log.Printf("Subscribed to topic: %s", fullTopic)
		}
	}
}

func (c *KNMQTTClient) mqttMessageHandler(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received MQTT message on topic: %s", msg.Topic())

	// Convert MQTT payload to a CloudEvent
	newEvent := event.New() // Renamed 'event' to 'newEvent' for clarity
	payloadBytes := msg.Payload()
	fmt.Printf("payload: %s\n", payloadBytes)
	// ce := &event.Event{}
	err := json.Unmarshal(payloadBytes, &newEvent)
	log.Printf("MSG: %s\n", msg.Payload())
	log.Printf("payloadBytes: %s\n", payloadBytes)
	log.Printf("newEvent: %s\n", newEvent)
	if err != nil {
		log.Printf("MSG: %s\n", msg.Payload())
		log.Printf("payloadBytes: %s\n", payloadBytes)
		log.Printf("failed to unmarshal cloudevent: %s\n", err)
		return
	}
	// // Attempt to set the payload as JSON data for the CloudEvent.
	// // If the payloadBytes are not valid JSON, this call will return an error.
	// if err := newEvent.SetData(event.ApplicationJSON, payloadBytes); err != nil {
	// 	// Log a more specific error including the topic and the problematic payload
	// 	log.Printf("Failed to set event data from MQTT payload for topic %s: %v. Payload (as string): %s", msg.Topic(), err, string(payloadBytes))
	// 	return
	// }

	// // Set required CloudEvent attributes
	// newEvent.SetSource("mqtt-source")                                 // Consistent with Python's origin
	// newEvent.SetType("envds.mqtt.message")                            // A type for MQTT-originated messages
	// newEvent.SetID(fmt.Sprintf("mqtt-msg-%d", time.Now().UnixNano())) // Unique ID for the event

	// Set the 'sourcepath' extension, which was used in the Python version
	newEvent.SetExtension("sourcepath", msg.Topic())
	// // Set the 'sourcepath' extension, which was used in the Python version
	// ce.SetExtension("sourcepath", msg.Topic())
    fmt.Printf("Received CloudEvent (structured mode): %+v\n", newEvent)
    // Access event attributes and data:
    fmt.Printf("ID: %s\n", newEvent.ID())
    fmt.Printf("Source: %s\n", newEvent.Source())
    fmt.Printf("Type: %s\n", newEvent.Type())
    fmt.Printf("Data: %s\n", newEvent.Data())

	// Send the constructed CloudEvent to the channel for processing by the Knative sender loop
	c.toKnativeChannel <- newEvent
// 	// Send the constructed CloudEvent to the channel for processing by the Knative sender loop
// 	c.toKnativeChannel <- *ce
// }

// old
// // mqttMessageHandler receives messages from MQTT and sends them to the Knative channel.
// func (c *KNMQTTClient) mqttMessageHandler(client mqtt.Client, msg mqtt.Message) {
// 	log.Printf("Received MQTT message on topic: %s", msg.Topic())

// 	// Convert MQTT payload to a CloudEvent
// 	event := event.New()
// 	if err := event.SetData(event.ApplicationJSON, msg.Payload()); err != nil {
// 		log.Printf("Failed to set event data: %v", err)
// 		return
// 	}
// 	event.SetSource("mqtt-source")
// 	event.SetType("envds.mqtt.message")
// 	event.SetID(fmt.Sprintf("mqtt-msg-%d", time.Now().UnixNano()))
// 	event.SetExtension("sourcepath", msg.Topic())

// 	c.toKnativeChannel <- event
}

// sendToMqttLoop publishes messages from the channel to MQTT.
func (c *KNMQTTClient) sendToMqttLoop() {
	for ce := range c.toMqttChannel {
		destpath, _ := ce.Extensions()["destpath"].(string)
		if destpath == "" {
			log.Printf("CloudEvent missing 'destpath' extension, cannot publish to MQTT")
			continue
		}

		body := ce.Data()
		// if err != nil {
		// 	log.Printf("Error getting CloudEvent data: %v", err)
		// 	continue
		// }

		token := c.mqttClient.Publish(destpath, 1, false, body)
		token.Wait()
		if token.Error() != nil {
			log.Printf("Error publishing to MQTT topic %s: %v", destpath, token.Error())
		} else {
			log.Printf("Published message to MQTT topic: %s", destpath)
		}
	}
}

// sendToKnativeLoop sends CloudEvents from the channel to the Knative broker.
func (c *KNMQTTClient) sendToKnativeLoop() {
	// FIX: Use the aliased package 'cehttp' to create the CloudEvents HTTP client
	client, err := cloudevents.NewClientHTTP()
	if err != nil {
		log.Fatalf("failed to create http client: %s", err)
	}

	for ce := range c.toKnativeChannel {
		// ctx := context.Background()
		ctx := cloudevents.ContextWithTarget(context.Background(), c.config.KnativeBroker)
		if result := client.Send(ctx, ce); !protocol.IsACK(result) {
			log.Printf("Failed to send CloudEvent to Knative broker: %v", result)
		} else {
			log.Printf("Successfully sent CloudEvent to Knative broker: %s", ce)
		}
	}
}

// mqttSendHandler receives HTTP POST requests, converts them to CloudEvents, and
// sends them to the MQTT channel.
func (c *KNMQTTClient) mqttSendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	event, err := cehttp.NewEventFromHTTPRequest(r)
	if err != nil {
		log.Printf("Error receiving CloudEvent from HTTP: %v", err)
		http.Error(w, "Invalid CloudEvent", http.StatusBadRequest)
		return
	}

	log.Printf("Received CloudEvent from Knative: %s", event.ID())
	c.toMqttChannel <- *event

	w.WriteHeader(http.StatusAccepted)
}

func main() {
	var config KNMQTTAdapterSettings
	if err := envconfig.Process("", &config); err != nil {
		log.Fatalf("Failed to process environment variables: %v", err)
	}
	log.Printf("Configuration: %+v", config)

	client := NewKNMQTTClient(config)
	defer client.mqttClient.Disconnect(250)

	// Set up the HTTP server to receive CloudEvents from Knative
	http.HandleFunc("/mqtt/send/", client.mqttSendHandler)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "ok")
	})

	log.Printf("Starting server on port %d...", config.Port)
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", config.Port),
		Handler: nil,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s: %v", server.Addr, err)
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("Server gracefully stopped")
}
