#!/bin/bash
# GoMQTT Cluster Setup Script
# This script automates the setup of a GoMQTT cluster with HAProxy load balancing

set -e # Exit on any error

# Default values
NODE_COUNT=3
OUTPUT_DIR="."
CONFIG_DIR="config"
CERTS_DIR="certs"
HAPROXY_DIR="haproxy"

# Print header
echo "========================================"
echo "   GoMQTT Cluster Setup Script"
echo "========================================"
echo ""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -n|--nodes)
      NODE_COUNT="$2"
      shift 2
      ;;
    -o|--output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: setup-cluster.sh [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  -n, --nodes NUMBER       Number of cluster nodes (default: 3)"
      echo "  -o, --output-dir PATH    Output directory (default: current directory)"
      echo "  -h, --help               Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "Setting up a ${NODE_COUNT}-node GoMQTT cluster in ${OUTPUT_DIR}"

# Create directories
mkdir -p "${OUTPUT_DIR}/${CONFIG_DIR}"
mkdir -p "${OUTPUT_DIR}/${CERTS_DIR}"
mkdir -p "${OUTPUT_DIR}/${HAPROXY_DIR}"
mkdir -p "${OUTPUT_DIR}/scripts"

# Generate certificate script
cat > "${OUTPUT_DIR}/scripts/generate-certs.sh" << 'EOF'
#!/bin/bash

set -e

CERTS_DIR=${1:-"certs"}
mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

echo "Generating certificates in $(pwd)..."

# Generate CA key and certificate
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt -subj "/CN=GoMQTT CA"

# Generate server key and CSR
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/CN=gomqtt"

# Generate server certificate
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256

# Create combined PEM file for HAProxy
cat server.crt server.key > combined.pem

echo "Certificates generated successfully in $(pwd)"
EOF

chmod +x "${OUTPUT_DIR}/scripts/generate-certs.sh"

# Generate certificates
echo "Generating TLS certificates..."
"${OUTPUT_DIR}/scripts/generate-certs.sh" "${OUTPUT_DIR}/${CERTS_DIR}"

# Generate HAProxy configuration
echo "Generating HAProxy configuration..."
cat > "${OUTPUT_DIR}/${HAPROXY_DIR}/haproxy.cfg" << EOF
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
EOF

# Add server entries to HAProxy config
for i in $(seq 1 $NODE_COUNT); do
  echo "    server node${i} gomqtt-node${i}:1883 check" >> "${OUTPUT_DIR}/${HAPROXY_DIR}/haproxy.cfg"
done

# Add WebSocket backend
echo "" >> "${OUTPUT_DIR}/${HAPROXY_DIR}/haproxy.cfg"
echo "backend ws_back" >> "${OUTPUT_DIR}/${HAPROXY_DIR}/haproxy.cfg"
echo "    mode tcp" >> "${OUTPUT_DIR}/${HAPROXY_DIR}/haproxy.cfg"
echo "    balance roundrobin" >> "${OUTPUT_DIR}/${HAPROXY_DIR}/haproxy.cfg"

for i in $(seq 1 $NODE_COUNT); do
  echo "    server node${i} gomqtt-node${i}:9001 check" >> "${OUTPUT_DIR}/${HAPROXY_DIR}/haproxy.cfg"
done

# Generate configuration for each node
echo "Generating node configurations..."
for i in $(seq 1 $NODE_COUNT); do
  # Determine seed nodes (all previous nodes)
  SEEDS=""
  if [ $i -gt 1 ]; then
    for j in $(seq 1 $(($i-1))); do
      if [ -n "$SEEDS" ]; then
        SEEDS="${SEEDS},"
      fi
      SEEDS="${SEEDS}gomqtt-node${j}:7946"
    done
  fi

  # Create node configuration
  cat > "${OUTPUT_DIR}/${CONFIG_DIR}/cluster-node${i}.json" << EOF
{
  "mqtt": {
    "host": "0.0.0.0",
    "port": 1883,
    "max_connections": 100000,
    "websocket": {
      "enabled": true,
      "port": 9001
    },
    "tls": {
      "enabled": true,
      "port": 8883,
      "cert_file": "certs/server.crt",
      "key_file": "certs/server.key"
    }
  },
  "database": {
    "type": "postgres",
    "host": "postgres",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "db_name": "gomqtt",
    "max_connections": 20
  },
  "cluster": {
    "enabled": true,
    "node_id": "node${i}",
    "node_host": "gomqtt-node${i}",
    "node_port": 7946,
    "gossip_port": 7947,
    "sync_interval": 30,
    "seed_nodes": [${SEEDS}]
  },
  "logging": {
    "level": "info",
    "format": "json"
  },
  "metrics": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 9090,
    "path": "/metrics"
  }
}
EOF
done

# Create API Gateway config
cat > "${OUTPUT_DIR}/${CONFIG_DIR}/api-config.json" << EOF
{
  "api": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8080,
    "base_path": "/api",
    "cors": {
      "enabled": true,
      "allowed_origins": ["*"]
    }
  },
  "database": {
    "type": "postgres",
    "host": "postgres",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "db_name": "gomqtt"
  },
  "logging": {
    "level": "info",
    "format": "json"
  }
}
EOF

# Create Admin UI config
cat > "${OUTPUT_DIR}/${CONFIG_DIR}/admin-config.json" << EOF
{
  "admin": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8081,
    "base_path": "/"
  },
  "database": {
    "type": "postgres",
    "host": "postgres",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "db_name": "gomqtt"
  },
  "logging": {
    "level": "info",
    "format": "json"
  }
}
EOF

# Generate docker-compose file
echo "Generating Docker Compose configuration..."
cat > "${OUTPUT_DIR}/docker-compose-cluster.yml" << EOF
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
EOF

# Add dependencies on all nodes
for i in $(seq 1 $NODE_COUNT); do
  echo "      - gomqtt-node${i}" >> "${OUTPUT_DIR}/docker-compose-cluster.yml"
done

# Add node services to docker-compose
for i in $(seq 1 $NODE_COUNT); do
  cat >> "${OUTPUT_DIR}/docker-compose-cluster.yml" << EOF

  gomqtt-node${i}:
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - ./config/cluster-node${i}.json:/app/config.json:ro
      - ./certs:/app/certs:ro
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - gomqtt_network
    environment:
      - GOMQTT_NODE_ID=node${i}
      - GOMQTT_NODE_HOST=gomqtt-node${i}
      - GOMQTT_CLUSTER_ENABLED=true
EOF

  # Add seed nodes environment variable if needed
  if [ $i -gt 1 ]; then
    SEEDS=""
    for j in $(seq 1 $(($i-1))); do
      if [ -n "$SEEDS" ]; then
        SEEDS="${SEEDS},"
      fi
      SEEDS="${SEEDS}gomqtt-node${j}:7946"
    done
    echo "      - GOMQTT_CLUSTER_SEEDS=${SEEDS}" >> "${OUTPUT_DIR}/docker-compose-cluster.yml"
  fi
done

# Add remaining services
cat >> "${OUTPUT_DIR}/docker-compose-cluster.yml" << EOF

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
EOF

echo "========================================"
echo "Setup completed successfully!"
echo "========================================"
echo ""
echo "Configuration files generated in: ${OUTPUT_DIR}"
echo ""
echo "To start the cluster, run:"
echo "cd ${OUTPUT_DIR} && docker-compose -f docker-compose-cluster.yml up -d"
echo ""
echo "Cluster endpoints:"
echo "- MQTT:      localhost:1883"
echo "- MQTTS:     localhost:8883 (TLS)"
echo "- WebSocket: localhost:9001"
echo "- WSS:       localhost:9443 (Secure WebSocket)"
echo "- API:       localhost:8080/api"
echo "- Admin UI:  localhost:8081"
echo "- HAProxy:   localhost:8404 (Statistics)"
echo ""
echo "For more information, see the documentation at:"
echo "https://github.com/mstgnz/gomqtt/cluster/cluster-setup.md" 