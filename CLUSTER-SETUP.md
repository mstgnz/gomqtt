# GoMQTT Multi-Node Deployment Guide

This guide explains how to set up a scalable multi-node deployment of GoMQTT using Docker Compose with HAProxy for load balancing.

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

## Setup Steps

### 1. Generate TLS Certificates

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

### 2. Configure the Nodes

The configuration for each node is provided in the `config/` directory:

- `cluster-node1.json` - Configuration for Node 1
- `cluster-node2.json` - Configuration for Node 2
- `cluster-node3.json` - Configuration for Node 3
- `api-config.json` - Configuration for the API Gateway

You can edit these files to adjust settings as needed.

### 3. Start the Cluster

Launch the entire cluster with:

```bash
docker-compose -f docker-compose-cluster.yml up -d
```

Wait for all services to start:

```bash
docker-compose -f docker-compose-cluster.yml ps
```

### 4. Test the Cluster

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
   server node4 gomqtt-node4:1883 check
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

## Monitoring

- HAProxy Stats: `http://localhost:8404`
- Admin UI: `http://localhost:8081`
- API metrics: `http://localhost:8080/metrics`
