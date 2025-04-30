#!/bin/bash
# Script to deploy GoMQTT cluster

set -e  # Exit on error

echo "===== GoMQTT Cluster Deployment ====="
echo "This script will deploy a multi-node GoMQTT cluster using Docker Compose."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker and try again."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Create necessary directories
mkdir -p haproxy/certs certs config scripts

# Check if the certificate generation script exists
if [ ! -f "scripts/generate-certs.sh" ]; then
    echo "Certificate generation script not found. Please ensure this file exists."
    exit 1
fi

# Make script executable if it isn't already
chmod +x scripts/generate-certs.sh

# Generate TLS certificates
echo -e "\n===== Generating TLS Certificates ====="
./scripts/generate-certs.sh

# Check if the docker-compose file exists
if [ ! -f "docker-compose-cluster.yml" ]; then
    echo "docker-compose-cluster.yml not found. Please ensure this file exists."
    exit 1
fi

# Start the cluster
echo -e "\n===== Starting the GoMQTT Cluster ====="
docker-compose -f docker-compose-cluster.yml up -d

# Wait for everything to start
echo -e "\n===== Waiting for services to start ====="
sleep 10

# Show container status
echo -e "\n===== Cluster Status ====="
docker-compose -f docker-compose-cluster.yml ps

echo -e "\n===== Deployment Complete ====="
echo "- MQTT Broker (TCP): localhost:1883"
echo "- MQTT Broker (TLS): localhost:8883"
echo "- MQTT Broker (WebSocket): localhost:9001"
echo "- MQTT Broker (WSS): localhost:9443"
echo "- API: http://localhost:8080"
echo "- Admin UI: http://localhost:8081"
echo "- HAProxy Stats: http://localhost:8404"
echo -e "\nFor detailed troubleshooting, see CLUSTER-SETUP.md" 