To install Knative [eventing](https://knative.dev/docs/install/yaml-install/eventing/install-eventing-with-yaml/#prerequisites):
```bash
kubectl apply -f https://github.com/knative/eventing/releases/download/knative-v1.18.1/eventing-crds.yaml
```

```bash
kubectl apply -f https://github.com/knative/eventing/releases/download/knative-v1.18.1/eventing-core.yaml
```

For remote (fewer resources) systems, add in-memory channel and broker layer
```bash
kubectl apply -f https://github.com/knative/eventing/releases/download/knative-v1.18.1/in-memory-channel.yaml
kubectl apply -f https://github.com/knative/eventing/releases/download/knative-v1.18.1/mt-channel-broker.yaml
```
Add broker using this manifest file as an example:

```yaml
apiVersion: eventing.knative.dev/v1
kind: Broker
metadata:
  annotations:
    eventing.knative.dev/broker.class: MTChannelBasedBroker
  name: msp-broker-imc
  namespace: default # <- this might need to be knative-eventing
```

For larger clusters, add kafka channel and broker layer (need strimzi/kafka installed)
```bash
kubectl apply -f https://github.com/knative-extensions/eventing-kafka-broker/releases/download/knative-v1.18.0/eventing-kafka-controller.yaml
kubectl apply -f https://github.com/knative-extensions/eventing-kafka-broker/releases/download/knative-v1.18.0/eventing-kafka-broker.yaml
```

Add a broker using this manifest file as an example:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-broker-config
  namespace: knative-eventing
data:
  default.topic.partitions: "10"
  default.topic.replication.factor: "3"
  bootstrap.servers: "my-cluster-kafka-bootstrap.kafka:9092"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-channel-config
  namespace: knative-eventing
data:
  bootstrap.servers: "my-cluster-kafka-bootstrap.kafka:9092"
---
apiVersion: eventing.knative.dev/v1
kind: Broker
metadata:
  annotations:
    eventing.knative.dev/broker.class: Kafka
  name: default
  namespace: default
spec:
  # Configuration specific to this broker.
  config:
    apiVersion: v1
    kind: ConfigMap
    name: kafka-broker-config
    namespace: knative-eventing
  # Optional dead letter sink, you can specify either:
  #  - deadLetterSink.ref, which is a reference to a Callable
  #  - deadLetterSink.uri, which is an absolute URI to a Callable (It can potentially be out of the Kubernetes cluster)
  # delivery:
  #   deadLetterSink:
  #     ref:
  #       apiVersion: serving.knative.dev/v1
  #       kind: Service
  #       name: dlq-service
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: config-br-defaults
  namespace: knative-eventing
data:
  default-br-config: |
    clusterDefault:
      brokerClass: Kafka
      apiVersion: v1
      kind: ConfigMap
      name: kafka-broker-config
      namespace: knative-eventing
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: config-kafka-broker-data-plane
  namespace: knative-eventing
data:
  config-kafka-broker-consumer.properties: |
    # Commit the offset every 2000 millisecods (2 seconds)
    auto.commit.interval.ms=2000
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-config-logging
  namespace: knative-eventing
data:
  # Changing logging to DEBUG
  config.xml: |
    <configuration>
      <appender name="jsonConsoleAppender" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder"/>
      </appender>
      <root level="DEBUG">
        <appender-ref ref="jsonConsoleAppender"/>
      </root>
    </configuration>

```