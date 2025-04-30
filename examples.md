# 📚 GoMQTT Code Examples

This document contains various code examples for interacting with the GoMQTT broker.

## 📡 MQTT Client Examples

### React Native (for mobile devices)

```bash
npm install mqtt
```

```javascript
import mqtt from "mqtt";

const client = mqtt.connect("wss://broker-address:port");

client.on("connect", () => {
  client.subscribe("sensor/temperature");
  client.publish("sensor/temperature", "23°C");
});
```

### Go (for IoT devices)

```go
package main

import (
    MQTT "github.com/eclipse/paho.mqtt.golang"
    "fmt"
)

func main() {
    opts := MQTT.NewClientOptions().AddBroker("tcp://broker-address:1883")
    client := MQTT.NewClient(opts)

    if token := client.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }

    token := client.Publish("sensor/temperature", 0, false, "23°C")
    token.Wait()

    fmt.Println("Message published")
}
```

## 🔒 Secure Connection Examples

### Secure MQTT (MQTTS) Connection

```go
package main

import (
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "io/ioutil"

    MQTT "github.com/eclipse/paho.mqtt.golang"
)

func main() {
    // TLS configuration
    tlsConfig := &tls.Config{}

    // Verify certificates (can be set to false for self-signed certificates)
    tlsConfig.InsecureSkipVerify = false

    // Add CA certificate (for validating self-signed certificates)
    caCert, err := ioutil.ReadFile("certs/ca.crt")
    if err == nil {
        caCertPool := x509.NewCertPool()
        caCertPool.AppendCertsFromPEM(caCert)
        tlsConfig.RootCAs = caCertPool
    }

    // Client certificate (for mTLS)
    cert, err := tls.LoadX509KeyPair("certs/client.crt", "certs/client.key")
    if err == nil {
        tlsConfig.Certificates = []tls.Certificate{cert}
    }

    // MQTT client configuration
    opts := MQTT.NewClientOptions()
    opts.AddBroker("ssl://broker-address:8883")
    opts.SetClientID("secure-client")
    opts.SetTLSConfig(tlsConfig)

    client := MQTT.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }

    fmt.Println("Securely connected!")
}
```

### Secure WebSocket (WSS) Connection

```javascript
// WSS Connection with Paho MQTT Client
var mqttConfig = {
  host: "broker-address", // or window.location.hostname
  port: 9443,
  path: "/mqtt",
  clientId: "browser_" + Math.random().toString(16).substr(2, 8),
  useSSL: true,
};

// Create WSS connection
var client = new Paho.MQTT.Client(
  mqttConfig.host,
  mqttConfig.port,
  mqttConfig.path,
  mqttConfig.clientId
);

// Connection options
var options = {
  timeout: 3,
  keepAliveInterval: 60,
  useSSL: mqttConfig.useSSL,
  onSuccess: function () {
    console.log("Secure WebSocket connection established!");

    // Publish a message
    var message = new Paho.MQTT.Message("Hello Secure MQTT!");
    message.destinationName = "test/topic";
    client.send(message);
  },
  onFailure: function (e) {
    console.error("Connection error:", e.errorMessage);
  },
};

// Connect
client.connect(options);
```

## 🔧 Client Libraries

Here's a list of recommended MQTT client libraries for various platforms:

### JavaScript/Node.js

- [MQTT.js](https://github.com/mqttjs/MQTT.js)
- [Paho JavaScript Client](https://www.eclipse.org/paho/clients/js/)

### Python

- [paho-mqtt](https://pypi.org/project/paho-mqtt/)
- [gmqtt](https://github.com/wialon/gmqtt)

### Java

- [Eclipse Paho Java Client](https://www.eclipse.org/paho/clients/java/)
- [HiveMQ MQTT Client](https://github.com/hivemq/hivemq-mqtt-client)

### Go

- [Eclipse Paho MQTT Go Client](https://github.com/eclipse/paho.mqtt.golang)
- [Gmqtt](https://github.com/DrmagicE/gmqtt)

### C/C++

- [Eclipse Paho C Client](https://www.eclipse.org/paho/clients/c/)
- [Eclipse Paho C++ Client](https://www.eclipse.org/paho/clients/cpp/)
- [MQTT-C](https://github.com/LiamBindle/MQTT-C)

### C#/.NET

- [MQTTnet](https://github.com/dotnet/MQTTnet)
- [M2Mqtt](https://www.nuget.org/packages/M2Mqtt/)

### Rust

- [rumqtt](https://github.com/bytebeamio/rumqtt)
- [paho-mqtt-rust](https://github.com/eclipse/paho.mqtt.rust)

### Mobile

- Android: [Eclipse Paho Android Service](https://github.com/eclipse/paho.mqtt.android)
- iOS: [CocoaMQTT](https://github.com/emqx/CocoaMQTT)

## 🔌 Application Examples

### Home Automation System

```javascript
// Connect to the broker
const mqtt = require("mqtt");
const client = mqtt.connect("mqtt://localhost:1883", {
  username: "homeautomation",
  password: "secret-password",
});

// Subscribe to home automation topics
client.on("connect", () => {
  console.log("Connected to MQTT broker");
  client.subscribe("home/+/switch");
  client.subscribe("home/+/temperature");
  client.subscribe("home/+/humidity");
});

// Listen for messages
client.on("message", (topic, message) => {
  console.log(`Received message on ${topic}: ${message.toString()}`);

  // Parse the topic to extract device type and ID
  const topicParts = topic.split("/");
  const location = topicParts[1];
  const measureType = topicParts[2];

  // Process based on the measurement type
  switch (measureType) {
    case "switch":
      console.log(`Switch in ${location} changed to ${message}`);
      break;
    case "temperature":
      console.log(`Temperature in ${location}: ${message}°C`);
      break;
    case "humidity":
      console.log(`Humidity in ${location}: ${message}%`);
      break;
  }
});

// Publish a command to turn on the living room light
function turnOnLight(room) {
  client.publish(`home/${room}/switch/command`, "ON");
}

// Example usage
// turnOnLight('living-room');
```
