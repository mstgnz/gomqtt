# 🔄 GoMQTT Cluster Setup Guide

This guide explains how to set up a scalable multi-node deployment of GoMQTT using Docker Compose with HAProxy for load balancing.

## Clustering Overview

GoMQTT supports clustering to provide high availability and scalability. Multiple GoMQTT brokers can be connected together to form a cluster. The following features are supported in cluster mode:

- **Automatic node discovery**: Nodes automatically discover and connect to each other
- **State synchronization**: Subscriptions and retained messages are synchronized across the cluster
- **Message sharing**: Messages published to one node are distributed to subscribers on other nodes
- **Shared subscriptions**: Multiple subscribers can share a subscription across different nodes
- **High availability**: If one node fails, clients can connect to another node in the cluster

## Architecture Overview

The deployment consists of:

1. **Multiple GoMQTT Broker Nodes**: Independent MQTT brokers that form a cluster
2. **HAProxy Load Balancer**: Distributes client connections across the cluster
3. **PostgreSQL Database**: Shared storage for message persistence and session data
4. **API Gateway**: Single entry point for REST API access
5. **Admin UI**: Web interface for monitoring and management

## Prerequisites

- Docker and Docker Compose installed
- Basic understanding of MQTT, TLS, and networking

## Basic Cluster Configuration

To enable clustering, update your configuration file:

```json
{
  "cluster": {
    "enabled": true,
    "node_id": "node1", // Unique identifier for this node
    "node_host": "localhost", // Host address for cluster communication
    "node_port": 7946, // Port for cluster communication (Memberlist default)
    "gossip_port": 7947, // Port for gossip protocol
    "seed_nodes": [
      // List of existing nodes to join
      "node2:7946",
      "node3:7946"
    ],
    "sync_interval": 30 // Synchronization interval in seconds
  }
}
```

## Multi-Node Deployment with Docker Compose

For production environments, GoMQTT provides a ready-to-use multi-node deployment configuration with Docker Compose.

### Setup Steps

#### 1. Generate TLS Certificates

Run the provided script to generate self-signed certificates:

```bash
mkdir -p scripts
chmod +x scripts/generate-certs.sh
./scripts/generate-certs.sh
```

This creates:

- A Certificate Authority (CA) certificate
- Server certificates for TLS connections
- A combined PEM file for HAProxy

#### 2. Configure the Nodes

The configuration for each node is provided in the `config/` directory:

- `cluster-node1.json` - Configuration for Node 1
- `cluster-node2.json` - Configuration for Node 2
- `cluster-node3.json` - Configuration for Node 3
- `api-config.json` - Configuration for the API Gateway

You can edit these files to adjust settings as needed.

#### 3. Start the Cluster

Launch the entire cluster with:

```bash
docker-compose -f docker-compose-cluster.yml up -d
```

Wait for all services to start:

```bash
docker-compose -f docker-compose-cluster.yml ps
```

#### 4. Test the Cluster

Connect MQTT clients to HAProxy (port 1883) which will distribute connections to the broker nodes:

```bash
# Standard MQTT
mqtt-client-tool -h localhost -p 1883

# MQTT over TLS
mqtt-client-tool -h localhost -p 8883 --tls

# MQTT over WebSocket
mqtt-client-tool -h localhost -p 9001 --ws
```

Access the HAProxy stats dashboard at `http://localhost:8404` to monitor connection distribution.

## Docker Compose Configuration

Here's the structure of the `docker-compose-cluster.yml` file:

```yaml
version: "3"

services:
  haproxy:
    image: haproxy:latest
    ports:
      - "1883:1883" # MQTT
      - "8883:8883" # MQTTS
      - "9001:9001" # WebSocket
      - "9443:9443" # Secure WebSocket
      - "8404:8404" # HAProxy stats
    volumes:
      - ./haproxy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
      - ./certs/combined.pem:/usr/local/etc/haproxy/certs/combined.pem:ro
    networks:
      - gomqtt_network
    depends_on:
      - gomqtt-node1
      - gomqtt-node2
      - gomqtt-node3

  gomqtt-node1:
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - ./config/cluster-node1.json:/app/config.json:ro
      - ./certs:/app/certs:ro
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - gomqtt_network
    environment:
      - GOMQTT_NODE_ID=node1
      - GOMQTT_NODE_HOST=gomqtt-node1
      - GOMQTT_CLUSTER_ENABLED=true
      - GOMQTT_CLUSTER_SEEDS=

  gomqtt-node2:
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - ./config/cluster-node2.json:/app/config.json:ro
      - ./certs:/app/certs:ro
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - gomqtt_network
    environment:
      - GOMQTT_NODE_ID=node2
      - GOMQTT_NODE_HOST=gomqtt-node2
      - GOMQTT_CLUSTER_ENABLED=true
      - GOMQTT_CLUSTER_SEEDS=gomqtt-node1:7946

  gomqtt-node3:
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - ./config/cluster-node3.json:/app/config.json:ro
      - ./certs:/app/certs:ro
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - gomqtt_network
    environment:
      - GOMQTT_NODE_ID=node3
      - GOMQTT_NODE_HOST=gomqtt-node3
      - GOMQTT_CLUSTER_ENABLED=true
      - GOMQTT_CLUSTER_SEEDS=gomqtt-node1:7946,gomqtt-node2:7946

  api-gateway:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8080:8080" # API port
    volumes:
      - ./config/api-config.json:/app/config.json:ro
      - ./certs:/app/certs:ro
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - gomqtt_network
    command: ["./gomqtt", "-mode", "api"]

  admin-ui:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8081:8081" # Admin UI port
    volumes:
      - ./config/admin-config.json:/app/config.json:ro
      - ./certs:/app/certs:ro
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - gomqtt_network
    command: ["./gomqtt", "-mode", "admin"]

  postgres:
    image: postgres:16
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=gomqtt
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - gomqtt_network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

networks:
  gomqtt_network:
    driver: bridge

volumes:
  postgres_data:
```

## HAProxy Configuration

The HAProxy configuration for load balancing is defined in `haproxy/haproxy.cfg`:

```
global
    log stdout format raw local0
    maxconn 50000
    nbproc 1
    nbthread 4
    ssl-default-bind-ciphersuites TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256
    ssl-default-bind-options no-sslv3 no-tlsv10 no-tlsv11

defaults
    mode tcp
    timeout connect 5s
    timeout client 30s
    timeout server 30s
    log global

frontend mqtt_front
    bind *:1883
    mode tcp
    default_backend mqtt_back

frontend mqtts_front
    bind *:8883 ssl crt /usr/local/etc/haproxy/certs/combined.pem
    mode tcp
    default_backend mqtt_back

frontend ws_front
    bind *:9001
    mode tcp
    default_backend ws_back

frontend wss_front
    bind *:9443 ssl crt /usr/local/etc/haproxy/certs/combined.pem
    mode tcp
    default_backend ws_back

frontend stats
    bind *:8404
    mode http
    stats enable
    stats uri /
    stats refresh 10s
    stats admin if TRUE

backend mqtt_back
    mode tcp
    balance roundrobin
    server node1 gomqtt-node1:1883 check
    server node2 gomqtt-node2:1883 check
    server node3 gomqtt-node3:1883 check

backend ws_back
    mode tcp
    balance roundrobin
    server node1 gomqtt-node1:9001 check
    server node2 gomqtt-node2:9001 check
    server node3 gomqtt-node3:9001 check
```

## Scaling the Cluster

To add more nodes:

1. Create a new configuration file for the node:

   ```bash
   cp config/cluster-node1.json config/cluster-node4.json
   ```

2. Edit the config to change the node ID and update any node-specific settings.

3. Add the new node to `docker-compose-cluster.yml`:

   ```yaml
   gomqtt-node4:
     build:
       context: .
       dockerfile: Dockerfile
     volumes:
       - ./config/cluster-node4.json:/app/config.json:ro
       - ./certs:/app/certs:ro
     depends_on:
       postgres:
         condition: service_healthy
     networks:
       - gomqtt_network
     environment:
       - GOMQTT_NODE_ID=node4
       - GOMQTT_NODE_HOST=gomqtt-node4
       - GOMQTT_CLUSTER_ENABLED=true
       - GOMQTT_CLUSTER_SEEDS=gomqtt-node1:7946,gomqtt-node2:7946,gomqtt-node3:7946
   ```

4. Add the new node to HAProxy configuration:

   ```
   backend mqtt_back
     server node4 gomqtt-node4:1883 check

   backend ws_back
     server node4 gomqtt-node4:9001 check
   ```

5. Restart the cluster:
   ```bash
   docker-compose -f docker-compose-cluster.yml up -d
   ```

## Troubleshooting

### Node Connectivity Issues

Check if nodes can communicate with each other:

```bash
docker-compose -f docker-compose-cluster.yml exec gomqtt-node1 ping gomqtt-node2
```

### TLS Certificate Problems

If clients can't connect with TLS, verify the certificates:

```bash
openssl verify -CAfile certs/ca.crt certs/server.crt
```

### HAProxy Distribution

Check the HAProxy stats page at `http://localhost:8404` to ensure connections are being distributed properly.

## Performance Tuning

For high-load environments:

1. Increase the `max_connections` setting in each node's configuration
2. Adjust the rate limiting settings as needed
3. Consider using connection pooling for database operations
4. Scale the number of broker nodes horizontally
5. Optimize HAProxy settings, particularly `maxconn` and `timeout` values

## Monitoring

GoMQTT exports Prometheus metrics that can be used to monitor the cluster. The metrics provide valuable insights into the performance and health of your MQTT broker deployment.

### Setting Up Monitoring

A dedicated Docker Compose file is provided in the `metrics` directory for setting up Prometheus and Grafana:

```bash
cd metrics
docker-compose up -d
```

This will start:

- **Prometheus**: For collecting and storing metrics (available at http://localhost:9090)
- **Grafana**: For visualizing metrics with pre-configured dashboards (available at http://localhost:3000)
- **Node Exporter**: For collecting host system metrics
- **HAProxy Exporter**: For collecting HAProxy metrics

### Available Dashboards

Two pre-configured Grafana dashboards are provided:

1. **GoMQTT Metrics Dashboard**: Shows metrics for individual broker instances including:

   - Connected clients
   - Message rates
   - Memory and CPU usage
   - Subscription counts
   - Authentication statistics

2. **GoMQTT Cluster Dashboard**: Shows cluster-specific metrics including:
   - Active nodes
   - Message distribution across nodes
   - Client distribution
   - Per-node resource usage

### Accessing Dashboards

1. Open Grafana at http://localhost:3000 (default login: admin/admin)
2. Navigate to Dashboards → GoMQTT folder
3. Select either the "GoMQTT Metrics Dashboard" or "GoMQTT Cluster Dashboard"

### Alerts

Prometheus is configured with alerting rules for critical conditions:

- High CPU/memory usage
- Node failures
- Excessive connections
- Connection spikes
- High message latency

To view configured alerts, visit Prometheus at http://localhost:9090 and navigate to the "Alerts" section.

### Metrics Endpoint Configuration

Each GoMQTT node exposes its metrics on port 9090 by default. Make sure each node's configuration includes:

```json
{
  "metrics": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 9090,
    "path": "/metrics"
  }
}
```

## Monitoring Key Metrics

The most important metrics to monitor for a production deployment:

- `gomqtt_connected_clients`: Current number of connected clients
- `gomqtt_messages_received_total` and `gomqtt_messages_sent_total`: Message throughput
- `gomqtt_system_memory_bytes` and `gomqtt_system_cpu_percent`: Resource usage
- `gomqtt_message_processing_seconds`: Message processing latency
- `gomqtt_cluster_nodes_active`: Number of active cluster nodes
