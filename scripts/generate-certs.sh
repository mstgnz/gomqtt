#!/bin/bash
# Script to generate self-signed certificates for GoMQTT

# Create directories if they don't exist
mkdir -p certs
mkdir -p haproxy/certs

# Generate CA key and certificate
echo "Generating CA key and certificate..."
openssl genrsa -out certs/ca.key 2048
openssl req -x509 -new -nodes -key certs/ca.key -sha256 -days 3650 \
  -out certs/ca.crt -subj "/C=US/ST=State/L=City/O=Organization/OU=Unit/CN=GoMQTT CA"

# Generate server key and certificate signing request
echo "Generating server key and CSR..."
openssl genrsa -out certs/server.key 2048
openssl req -new -key certs/server.key -out certs/server.csr \
  -subj "/C=US/ST=State/L=City/O=Organization/OU=Unit/CN=gomqtt"

# Create config for alternative names
cat > certs/server.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = gomqtt-node1
DNS.3 = gomqtt-node2
DNS.4 = gomqtt-node3
DNS.5 = haproxy
IP.1 = 127.0.0.1
EOF

# Sign the server certificate with our CA
echo "Signing server certificate with our CA..."
openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key \
  -CAcreateserial -out certs/server.crt -days 3650 -sha256 -extfile certs/server.ext

# Create PEM file for HAProxy (combines cert and key)
echo "Creating PEM file for HAProxy..."
cat certs/server.crt certs/server.key > haproxy/certs/server.pem

# Verify the certificates
echo "Verifying certificates..."
openssl verify -CAfile certs/ca.crt certs/server.crt

echo "Certificate generation complete!"
echo "Server cert: certs/server.crt"
echo "Server key: certs/server.key"
echo "CA cert: certs/ca.crt"
echo "HAProxy PEM: haproxy/certs/server.pem"

# Make the script executable
chmod +x scripts/generate-certs.sh 