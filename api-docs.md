# GoMQTT REST API Documentation

This document provides information about the REST API endpoints available in GoMQTT broker. The API allows you to monitor and manage the broker including clients, topics, subscriptions, and messages.

## API Documentation with Scalar

We use [Scalar](https://scalar.com/) for our API documentation. Scalar provides a modern, interactive documentation experience based on the OpenAPI specification.

![Scalar API Documentation](https://via.placeholder.com/800x400/0096FF/FFFFFF?text=GoMQTT+API+Documentation)

## Authentication

The API uses JWT (JSON Web Token) for authentication. To obtain a token, use the `/auth/login` endpoint with valid credentials. Include the token in subsequent requests as a Bearer token in the Authorization header.

```
Authorization: Bearer your-jwt-token
```

## Base URL

The base URL for all API endpoints is:

```
http://your-server:8080
```

## Available Endpoints

The API is organized around these main resources:

### Authentication

- **POST /auth/login** - Authenticate to the API

### Clients

- **GET /clients** - List all connected clients
- **GET /clients/{clientId}** - Get details about a specific client
- **DELETE /clients/{clientId}** - Disconnect a client
- **GET /clients/{clientId}/subscriptions** - Get client subscriptions

### Messages

- **GET /messages** - Get message history with filtering options
- **POST /messages** - Publish a message

### Topics

- **GET /topics** - List all active topics
- **GET /topics/{topic}** - Get details about a specific topic
- **GET /topics/{topic}/subscribers** - Get subscribers for a topic

### Statistics

- **GET /stats** - Get broker statistics

## Setting Up Scalar

To use Scalar for your GoMQTT API documentation:

1. Install Scalar CLI:

   ```bash
   npm install -g @scalar/cli
   ```

2. Start the documentation server:

   ```bash
   scalar serve api-docs.yaml
   ```

3. Open your browser at `http://localhost:8080` to view the documentation

## Using the API with Curl

Here are some examples of using the API with curl:

### Authentication

```bash
curl -X POST http://localhost:8080/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin_password"}'
```

### List Clients

```bash
curl -X GET http://localhost:8080/clients \
  -H "Authorization: Bearer your-jwt-token"
```

### Publish Message

```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-jwt-token" \
  -d '{
    "topic": "test/topic",
    "payload": "Hello MQTT!",
    "qos": 1,
    "retained": false
  }'
```

## Integrating With Your Applications

You can generate client libraries using OpenAPI Generator or Swagger Codegen to easily integrate with the API in your preferred programming language.

For more detailed information about request and response formats, see the Scalar documentation.
