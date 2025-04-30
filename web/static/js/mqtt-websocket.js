/**
 * GoMQTT WebSocket Client Example
 * 
 * This is a simple example of how to connect to the GoMQTT broker via WebSocket
 * using the Eclipse Paho MQTT JavaScript client.
 * 
 * Prerequisites:
 * - Include the Paho MQTT client library in your HTML:
 *   <script src="https://cdnjs.cloudflare.com/ajax/libs/paho-mqtt/1.1.0/paho-mqtt.min.js"></script>
 */

// MQTT Broker WebSocket configuration
const mqttConfig = {
  host: window.location.hostname || 'localhost',
  port: 9001,
  path: '/mqtt',
  clientId: 'browser_' + Math.random().toString(16).substr(2, 8),
  username: '', // Set if authentication is required
  password: '', // Set if authentication is required
  useSSL: false,
  reconnect: true,
  keepAliveInterval: 60,
  timeout: 5
};

// MQTT client instance
let mqttClient = null;

/**
 * Connect to the MQTT broker via WebSocket
 */
function connectMQTT() {
  // Create a client instance
  mqttClient = new Paho.MQTT.Client(
    mqttConfig.host,
    mqttConfig.port,
    mqttConfig.path,
    mqttConfig.clientId
  );
  
  // Set callback handlers
  mqttClient.onConnectionLost = onConnectionLost;
  mqttClient.onMessageArrived = onMessageArrived;
  
  // Connect the client
  console.log('Connecting to MQTT broker...');
  
  // Connection options
  const options = {
    onSuccess: onConnect,
    onFailure: onConnectFailure,
    keepAliveInterval: mqttConfig.keepAliveInterval,
    timeout: mqttConfig.timeout
  };
  
  // Add authentication if provided
  if (mqttConfig.username) {
    options.userName = mqttConfig.username;
    options.password = mqttConfig.password;
  }
  
  // Use TLS if needed
  if (mqttConfig.useSSL) {
    options.useSSL = true;
  }
  
  mqttClient.connect(options);
}

/**
 * MQTT Connection successful callback
 */
function onConnect() {
  console.log('Connected to MQTT broker');
  
  // Subscribe to topics
  mqttClient.subscribe('example/topic');
  console.log('Subscribed to example/topic');
  
  // Publish a message to show we're connected
  publishMessage('example/topic', 'Client connected: ' + mqttConfig.clientId);
  
  // Update UI if needed
  document.getElementById('mqtt-status').textContent = 'Connected';
  document.getElementById('mqtt-status').className = 'connected';
}

/**
 * MQTT Connection failure callback
 */
function onConnectFailure(response) {
  console.error('Connection failed: ' + response.errorMessage);
  
  // Update UI if needed
  document.getElementById('mqtt-status').textContent = 'Failed to connect';
  document.getElementById('mqtt-status').className = 'error';
  
  // Try to reconnect after a delay
  if (mqttConfig.reconnect) {
    setTimeout(connectMQTT, 5000);
  }
}

/**
 * MQTT Connection lost callback
 */
function onConnectionLost(response) {
  if (response.errorCode !== 0) {
    console.log('Connection lost: ' + response.errorMessage);
    
    // Update UI if needed
    document.getElementById('mqtt-status').textContent = 'Disconnected';
    document.getElementById('mqtt-status').className = 'disconnected';
    
    // Try to reconnect after a delay
    if (mqttConfig.reconnect) {
      setTimeout(connectMQTT, 5000);
    }
  }
}

/**
 * MQTT Message arrived callback
 */
function onMessageArrived(message) {
  console.log('Message received:');
  console.log(' Topic: ' + message.destinationName);
  console.log(' Payload: ' + message.payloadString);
  console.log(' QoS: ' + message.qos);
  console.log(' Retained: ' + message.retained);
  console.log(' Duplicate: ' + message.duplicate);
  
  // Update UI with the received message if needed
  const messageList = document.getElementById('message-list');
  if (messageList) {
    const item = document.createElement('li');
    item.textContent = `${message.destinationName}: ${message.payloadString}`;
    messageList.appendChild(item);
    
    // Limit the number of displayed messages
    while (messageList.childNodes.length > 10) {
      messageList.removeChild(messageList.firstChild);
    }
  }
}

/**
 * Publish a message to a topic
 */
function publishMessage(topic, message, qos = 0, retained = false) {
  if (!mqttClient || !mqttClient.isConnected()) {
    console.error('Cannot publish: Not connected to MQTT broker');
    return false;
  }
  
  // Create a new message
  const mqttMessage = new Paho.MQTT.Message(message);
  mqttMessage.destinationName = topic;
  mqttMessage.qos = Number(qos);
  mqttMessage.retained = Boolean(retained);
  
  // Send the message
  mqttClient.send(mqttMessage);
  console.log(`Published message: ${message} to topic: ${topic}`);
  return true;
}

/**
 * Disconnect from the MQTT broker
 */
function disconnectMQTT() {
  if (mqttClient && mqttClient.isConnected()) {
    mqttClient.disconnect();
    console.log('Disconnected from MQTT broker');
    
    // Update UI if needed
    document.getElementById('mqtt-status').textContent = 'Disconnected';
    document.getElementById('mqtt-status').className = 'disconnected';
  }
}

// Initialize the connection when the page loads
document.addEventListener('DOMContentLoaded', function() {
  // Connect to the MQTT broker
  connectMQTT();
  
  // Setup event handlers for UI elements
  const publishForm = document.getElementById('publish-form');
  if (publishForm) {
    publishForm.addEventListener('submit', function(e) {
      e.preventDefault();
      const topic = document.getElementById('publish-topic').value;
      const message = document.getElementById('publish-message').value;
      const qos = document.getElementById('publish-qos').value;
      const retained = document.getElementById('publish-retained').checked;
      
      publishMessage(topic, message, qos, retained);
      
      // Optionally reset the form
      document.getElementById('publish-message').value = '';
    });
  }
  
  // Setup disconnect button handler
  const disconnectButton = document.getElementById('disconnect-button');
  if (disconnectButton) {
    disconnectButton.addEventListener('click', disconnectMQTT);
  }
}); 