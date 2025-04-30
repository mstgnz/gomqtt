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

## OAuth2 Authentication Examples

### Google OAuth2 Integration

This example shows how to configure GoMQTT to use Google OAuth2 for authentication.

#### 1. Set up Google OAuth2 credentials

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Navigate to "APIs & Services" > "Credentials"
4. Click "Create Credentials" and select "OAuth client ID"
5. Set up the OAuth consent screen if prompted
6. For "Application type", choose "Web application"
7. Add the redirect URI: `http://localhost:8080/oauth/callback`
8. Click "Create" and note your Client ID and Client Secret

#### 2. Configure GoMQTT with Google OAuth2

Create a configuration file `config/google-oauth.json`:

```json
{
  "mqtt": {
    "host": "0.0.0.0",
    "port": 1883,
    "allow_anonymous": false
  },
  "auth": {
    "jwt_secret": "your-jwt-secret-here",
    "jwt_expires": 24,
    "oauth2": {
      "enabled": true,
      "client_id": "your-google-client-id",
      "client_secret": "your-google-client-secret",
      "auth_url": "https://accounts.google.com/o/oauth2/auth",
      "token_url": "https://oauth2.googleapis.com/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["email", "profile"],
      "user_info_url": "https://www.googleapis.com/oauth2/v3/userinfo",
      "token_field": "password",
      "username_field": "email"
    }
  }
}
```

#### 3. Start GoMQTT with the configuration

```bash
./gomqtt -config config/google-oauth.json
```

#### 4. Connect MQTT clients with OAuth2 tokens

**Python example using Paho MQTT:**

```python
import paho.mqtt.client as mqtt
import requests
import json
import webbrowser

# First, open the browser to get the OAuth2 authorization code
auth_url = "https://accounts.google.com/o/oauth2/auth?response_type=code&client_id=your-client-id&redirect_uri=http://localhost:8080/oauth/callback&scope=email profile"
webbrowser.open(auth_url)

# After authorization, you'll get a code in the callback URL
# Enter that code here:
code = input("Enter the authorization code from the callback URL: ")

# Exchange the code for a token
token_response = requests.post(
    "https://oauth2.googleapis.com/token",
    data={
        "code": code,
        "client_id": "your-client-id",
        "client_secret": "your-client-secret",
        "redirect_uri": "http://localhost:8080/oauth/callback",
        "grant_type": "authorization_code"
    }
)

token_data = token_response.json()
access_token = token_data["access_token"]

# Get user info to determine username
user_info = requests.get(
    "https://www.googleapis.com/oauth2/v3/userinfo",
    headers={"Authorization": f"Bearer {access_token}"}
)
user_data = user_info.json()
email = user_data["email"]

# Connect to MQTT broker with OAuth2 token
client = mqtt.Client()
client.username_pw_set(email, password=access_token)
client.connect("localhost", 1883, 60)

# Now you can publish/subscribe
client.publish("test/topic", "Hello with OAuth2!")
client.subscribe("test/topic")

# Start the loop
client.loop_forever()
```

**Node.js example using MQTT.js:**

```javascript
const mqtt = require("mqtt");
const axios = require("axios");
const open = require("open");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

// Configuration
const config = {
  clientId: "your-client-id",
  clientSecret: "your-client-secret",
  redirectUri: "http://localhost:8080/oauth/callback",
  authUrl: "https://accounts.google.com/o/oauth2/auth",
  tokenUrl: "https://oauth2.googleapis.com/token",
  userInfoUrl: "https://www.googleapis.com/oauth2/v3/userinfo",
  scope: "email profile",
};

// Open browser for authorization
const authUrl = `${config.authUrl}?response_type=code&client_id=${config.clientId}&redirect_uri=${config.redirectUri}&scope=${config.scope}`;
open(authUrl);

// Get authorization code from user
rl.question(
  "Enter the authorization code from the callback URL: ",
  async (code) => {
    try {
      // Exchange code for token
      const tokenResponse = await axios.post(config.tokenUrl, null, {
        params: {
          code,
          client_id: config.clientId,
          client_secret: config.clientSecret,
          redirect_uri: config.redirectUri,
          grant_type: "authorization_code",
        },
      });

      const accessToken = tokenResponse.data.access_token;

      // Get user info
      const userInfoResponse = await axios.get(config.userInfoUrl, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const email = userInfoResponse.data.email;

      // Connect to MQTT broker with OAuth2 credentials
      const client = mqtt.connect("mqtt://localhost:1883", {
        username: email,
        password: accessToken,
      });

      client.on("connect", () => {
        console.log("Connected to MQTT broker!");

        // Subscribe to a topic
        client.subscribe("test/topic", (err) => {
          if (!err) {
            // Publish a message
            client.publish("test/topic", "Hello with OAuth2!");
          }
        });
      });

      client.on("message", (topic, message) => {
        console.log(`Received message on ${topic}: ${message.toString()}`);
      });
    } catch (error) {
      console.error("Error:", error.response?.data || error.message);
    } finally {
      rl.close();
    }
  }
);
```

### Auth0 OAuth2 Integration

This example shows how to configure GoMQTT to use Auth0 for OAuth2 authentication.

#### 1. Set up Auth0 Application

1. Sign up for an [Auth0 account](https://auth0.com/)
2. Create a new Application (Regular Web Application)
3. Configure the Callback URL: `http://localhost:8080/oauth/callback`
4. Note your Domain, Client ID, and Client Secret

#### 2. Configure GoMQTT with Auth0

Create a configuration file `config/auth0-oauth.json`:

```json
{
  "mqtt": {
    "host": "0.0.0.0",
    "port": 1883,
    "allow_anonymous": false
  },
  "auth": {
    "jwt_secret": "your-jwt-secret-here",
    "jwt_expires": 24,
    "oauth2": {
      "enabled": true,
      "client_id": "your-auth0-client-id",
      "client_secret": "your-auth0-client-secret",
      "auth_url": "https://your-tenant.auth0.com/authorize",
      "token_url": "https://your-tenant.auth0.com/oauth/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["openid", "profile", "email"],
      "user_info_url": "https://your-tenant.auth0.com/userinfo",
      "token_field": "password",
      "username_field": "email"
    }
  }
}
```

The client connection process is similar to the Google example above, but using the Auth0 endpoints.
