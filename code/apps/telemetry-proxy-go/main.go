package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/oklog/ulid/v2"
	"golang.org/x/crypto/chacha20poly1305"
)

// --- CONFIGURATION ---
type ProxySettings struct {
	Host                 string
	Port                 int
	DaqID                string
	LogLevel             string
	TransmissionRateHz   float64
	MQTTBroker           string
	MQTTPort             int
	MQTTClientID         string
	LocalInterceptTopics string
	BridgeTopicOut       string
	BridgeTopicIn        string
	AESEncryptionKey     string
}

func loadConfig() ProxySettings {
	return ProxySettings{
		Host:                 getEnv("PROXY_HOST", "0.0.0.0"),
		Port:                 getEnvInt("PROXY_PORT", 8081),
		DaqID:                getEnv("PROXY_DAQ_ID", "default"),
		LogLevel:             getEnv("PROXY_LOG_LEVEL", "INFO"),
		TransmissionRateHz:   getEnvFloat("PROXY_TRANSMISSION_RATE_HZ", 5.0),
		MQTTBroker:           getEnv("PROXY_MQTT_BROKER", "mosquitto.default"),
		MQTTPort:             getEnvInt("PROXY_MQTT_PORT", 1883),
		MQTTClientID:         getEnv("PROXY_MQTT_CLIENT_ID", fmt.Sprintf("proxy-%s", generateULID())),
		LocalInterceptTopics: getEnv("PROXY_LOCAL_INTERCEPT_TOPICS", "envds/+/+/+/data/update,envds/+/+/+/status/update,envds/+/+/+/settings/update"),
		BridgeTopicOut:       getEnv("PROXY_BRIDGE_TOPIC_OUT", "envds/transport/compressed/edge-to-cloud"),
		BridgeTopicIn:        getEnv("PROXY_BRIDGE_TOPIC_IN", "envds/transport/compressed/cloud-to-edge"),
		AESEncryptionKey:     getEnv("PROXY_AES_ENCRYPTION_KEY", ""),
	}
}

// --- DATA STRUCTURES ---
type LocalMessage struct {
	T string
	D json.RawMessage
}

type PublishRequest struct {
	Topic      string
	Payload    []byte
	Properties *paho.PublishProperties
}

type BatchItem struct {
	T string          `json:"t"`
	D json.RawMessage `json:"d"`
}

type TelemetryProxy struct {
	config        ProxySettings
	cipher        cipher.AEAD
	outboundChan  chan PublishRequest
	inboundChan   chan *paho.Publish
	localBatchChan chan LocalMessage
	backfillChan  chan PublishRequest

	mu                 sync.Mutex
	totalOutboundBytes int
	totalRawBytes      int
}

func NewTelemetryProxy(cfg ProxySettings) *TelemetryProxy {
	keyBytes := []byte(cfg.AESEncryptionKey)
	paddedKey := make([]byte, 32)
	copy(paddedKey, keyBytes)

	aead, err := chacha20poly1305.New(paddedKey)
	if err != nil {
		log.Fatalf("Failed to initialize ChaCha20Poly1305: %v", err)
	}

	return &TelemetryProxy{
		config:        cfg,
		cipher:        aead,
		outboundChan:  make(chan PublishRequest, 10),
		inboundChan:   make(chan *paho.Publish, 100),
		localBatchChan: make(chan LocalMessage, 5000),
		backfillChan:  make(chan PublishRequest, 10000),
	}
}

// --- ROUTING & SHEDDING LOGIC ---
func (p *TelemetryProxy) pushToLocalBatch(msg LocalMessage) {
	select {
	case p.localBatchChan <- msg:
	default:
		select { case <-p.localBatchChan: default: }
		select { case p.localBatchChan <- msg: default: }
	}
}

func (p *TelemetryProxy) pushToInbound(pub *paho.Publish) {
	select {
	case p.inboundChan <- pub:
	default:
		select { case <-p.inboundChan: default: }
		select { case p.inboundChan <- pub: default: }
	}
}

func (p *TelemetryProxy) pushToOutbound(req PublishRequest) {
	select {
	case p.outboundChan <- req:
	default:
		var stale PublishRequest
		select {
		case stale = <-p.outboundChan:
			select {
			case p.backfillChan <- stale:
				log.Println("Outbound queue full. Shifting oldest batch to backfill queue.")
			default:
				log.Println("Backfill queue full. Dropping oldest telemetry permanently.")
			}
		default:
		}
		select {
		case p.outboundChan <- req:
		default:
		}
	}
}

func (p *TelemetryProxy) Setup(ctx context.Context) {
	go p.batchAggregatorWorker()
	go p.mqttLoop(ctx)
}

// --- CORE PIPELINES ---
func (p *TelemetryProxy) mqttLoop(ctx context.Context) {
	brokerURL, err := url.Parse(fmt.Sprintf("tcp://%s:%d", p.config.MQTTBroker, p.config.MQTTPort))
	if err != nil {
		log.Fatalf("Invalid broker URL: %v", err)
	}

	cliCfg := autopaho.ClientConfig{
		BrokerUrls: []*url.URL{brokerURL},
		KeepAlive:  30,
		ClientConfig: paho.ClientConfig{
			ClientID: p.config.MQTTClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					topic := pr.Packet.Topic
					if topic == p.config.BridgeTopicIn {
						p.pushToInbound(pr.Packet)
					} else {
						p.pushToLocalBatch(LocalMessage{T: topic, D: pr.Packet.Payload})
					}
					return true, nil
				},
			},
			OnClientError: func(err error) { log.Printf("MQTT Error: %v", err) },
		},
	}

	cliCfg.OnConnectionUp = func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
		log.Println("MQTT Connected. Starting Proxy pipelines.")

		// Subscriptions
		var subs []paho.SubscribeOptions
		if p.config.BridgeTopicIn != "" {
			subs = append(subs, paho.SubscribeOptions{Topic: p.config.BridgeTopicIn, QoS: 0})
		}
		for _, topic := range strings.Split(p.config.LocalInterceptTopics, ",") {
			t := strings.TrimSpace(topic)
			if t != "" {
				subs = append(subs, paho.SubscribeOptions{Topic: fmt.Sprintf("$share/proxy_group/%s", t), QoS: 0})
			}
		}

		if len(subs) > 0 {
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: subs}); err != nil {
				log.Printf("Failed to subscribe: %v", err)
			} else {
				log.Println("Proxy Subscriber listening on configured topics.")
			}
		}
	}

	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		log.Fatalf("Fatal MQTT setup error: %v", err)
	}

	for i := 0; i < 3; i++ {
		go p.publisherWorker(cm, i)
	}
	go p.inboundProcessorWorker(cm)
	go p.backfillPublisherWorker(cm)

	<-ctx.Done()
}

func (p *TelemetryProxy) batchAggregatorWorker() {
	flushInterval := time.Duration(float64(time.Second) / p.config.TransmissionRateHz)
	for {
		time.Sleep(flushInterval)

		var batchItems []string
		var currentSize int
		const maxBatchBytes = 128 * 1024

	drain:
		for {
			select {
			case msg := <-p.localBatchChan:
				itemStr := fmt.Sprintf(`{"t":%q,"d":%s}`, msg.T, string(msg.D))
				itemSize := len(itemStr)

				if len(batchItems) > 0 && currentSize+itemSize >= maxBatchBytes {
					itemsCopy := make([]string, len(batchItems))
					copy(itemsCopy, batchItems)
					go p.flushBatch(itemsCopy)
					batchItems = nil
					currentSize = 0
				}
				batchItems = append(batchItems, itemStr)
				currentSize += itemSize
			default:
				break drain
			}
		}

		if len(batchItems) > 0 {
			go p.flushBatch(batchItems)
		}
	}
}

func (p *TelemetryProxy) flushBatch(batchItems []string) {
	finalJSON := "[" + strings.Join(batchItems, ",") + "]"
	rawPayload := []byte(finalJSON)
	sizeIn := len(rawPayload)
	if sizeIn == 0 {
		return
	}

	tStart := time.Now()

	// Compress
	var buf bytes.Buffer
	zw, _ := zlib.NewWriterLevel(&buf, 6)
	zw.Write(rawPayload)
	zw.Close()
	compressed := buf.Bytes()

	// Encrypt
	nonce := make([]byte, chacha20poly1305.NonceSize)
	io.ReadFull(rand.Reader, nonce)
	ciphertext := p.cipher.Seal(nil, nonce, compressed, nil)
	finalPayload := append(nonce, ciphertext...)

	tElapsed := time.Since(tStart)
	if tElapsed > 50*time.Millisecond {
		log.Printf("Heavy batch compression (%d items) took %.2fms.", len(batchItems), float64(tElapsed.Microseconds())/1000.0)
	}

	sizeOut := len(finalPayload)
	p.mu.Lock()
	p.totalOutboundBytes += sizeOut
	p.totalRawBytes += sizeIn
	p.mu.Unlock()

	reduction := 0.0
	if sizeIn > 0 {
		reduction = (1.0 - float64(sizeOut)/float64(sizeIn)) * 100.0
	}
	log.Printf("compression_stats direction=outbound batch_items=%d bytes_in=%d bytes_out=%d reduction_pct=%.1f", len(batchItems), sizeIn, sizeOut, reduction)

	props := &paho.PublishProperties{
		ContentType: "application/octet-stream",
		User: []paho.UserProperty{
			{Key: "ce-specversion", Value: "1.0"},
			{Key: "ce-id", Value: generateULID()},
			{Key: "ce-type", Value: "envds.transport.batch"},
			{Key: "ce-source", Value: fmt.Sprintf("envds.%s.proxy", p.config.DaqID)},
			{Key: "txratehz", Value: fmt.Sprintf("%.1f", p.config.TransmissionRateHz)},
		},
	}

	p.pushToOutbound(PublishRequest{
		Topic:      p.config.BridgeTopicOut,
		Payload:    finalPayload,
		Properties: props,
	})
}

func (p *TelemetryProxy) inboundProcessorWorker(cm *autopaho.ConnectionManager) {
	for msg := range p.inboundChan {
		var originalTopic string
		if msg.Properties != nil {
			for _, up := range msg.Properties.User {
				if up.Key == "ce-originaltopic" {
					originalTopic = up.Value
					break
				}
			}
		}

		sizeIn := len(msg.Payload)
		if sizeIn < chacha20poly1305.NonceSize {
			log.Println("Inbound unpack error: payload too short")
			continue
		}

		// Decrypt
		nonce := msg.Payload[:chacha20poly1305.NonceSize]
		ciphertext := msg.Payload[chacha20poly1305.NonceSize:]
		compressed, err := p.cipher.Open(nil, nonce, ciphertext, nil)
		if err != nil {
			log.Printf("Inbound decrypt error: %v", err)
			continue
		}

		// Decompress
		zr, err := zlib.NewReader(bytes.NewReader(compressed))
		if err != nil {
			log.Printf("Inbound decompress error: %v", err)
			continue
		}
		originalJSONBytes, _ := io.ReadAll(zr)
		zr.Close()

		sizeOut := len(originalJSONBytes)
		reduction := 0.0
		if sizeOut > 0 {
			reduction = (1.0 - float64(sizeIn)/float64(sizeOut)) * 100.0
		}

		var batch []BatchItem
		if err := json.Unmarshal(originalJSONBytes, &batch); err == nil {
			log.Printf("Routing inbound batch of %d items. reduction_pct=%.1f", len(batch), reduction)
			for _, item := range batch {
				b, _ := json.Marshal(item.D)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				cm.Publish(ctx, &paho.Publish{Topic: item.T, QoS: 0, Payload: b})
				cancel()
			}
		} else {
			if originalTopic != "" {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				cm.Publish(ctx, &paho.Publish{Topic: originalTopic, QoS: 0, Payload: originalJSONBytes})
				cancel()
			}
		}
	}
}

func (p *TelemetryProxy) publisherWorker(cm *autopaho.ConnectionManager, workerID int) {
	for req := range p.outboundChan {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err := cm.Publish(ctx, &paho.Publish{
			Topic:      req.Topic,
			QoS:        0,
			Payload:    req.Payload,
			Properties: req.Properties,
		})
		cancel()
		if err != nil {
			log.Printf("Publisher %d timeout. Shedding packet due to network backpressure: %v", workerID, err)
		}
	}
}

func (p *TelemetryProxy) backfillPublisherWorker(cm *autopaho.ConnectionManager) {
	for req := range p.backfillChan {
		for len(p.outboundChan) > 0 {
			time.Sleep(200 * time.Millisecond)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err := cm.Publish(ctx, &paho.Publish{
			Topic:      req.Topic,
			QoS:        0,
			Payload:    req.Payload,
			Properties: req.Properties,
		})
		cancel()
		if err != nil {
			log.Printf("Backfill publish failed: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}

// --- UTILS & HTTP ---
func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func generateULID() string {
	t := time.Now().UTC()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy).String()
}

func main() {
	cfg := loadConfig()
	proxy := NewTelemetryProxy(cfg)
	proxy.Setup(context.Background())
	log.Println("Secure Telemetry Proxy initialized.")

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/events/inbound", func(w http.ResponseWriter, r *http.Request) {
		msg := cehttp.NewMessageFromHttpRequest(r)
		defer msg.Finish(nil)

		event, err := binding.ToEvent(context.Background(), msg)
		if err != nil {
			log.Printf("HTTP Ingest error: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		destpathIntf, err := event.Context.GetExtension("destpath")
		if err != nil || destpathIntf == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		destpath, ok := destpathIntf.(string)
		if !ok || destpath == "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		payloadBytes, _ := json.Marshal(event)
		proxy.pushToLocalBatch(LocalMessage{T: destpath, D: payloadBytes})
		w.WriteHeader(http.StatusNoContent)
	})

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	log.Fatal(http.ListenAndServe(addr, nil))
}