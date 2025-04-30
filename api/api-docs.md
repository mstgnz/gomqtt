# GoMQTT API Documentation

Welcome to the GoMQTT API documentation. This REST API allows you to manage and interact with your GoMQTT broker.

## Introduction

GoMQTT is a modern MQTT broker written in Go. It provides a full-featured MQTT server that supports MQTT protocol versions 3.1.1 and 5.0, along with additional management capabilities through this REST API.

The API provides functionality for:

- Managing user accounts and permissions
- Managing roles (RBAC)
- Publishing and retrieving messages
- Monitoring connected clients
- Exploring message history
- System health monitoring

## Authentication

All API endpoints (except for `/api/login`) require authentication using JWT tokens. To obtain a token, make a POST request to `/api/login` with your credentials.

```json
{
  "username": "your_username",
  "password": "your_password"
}
```

The response will include a JWT token that should be included in all subsequent requests in the `Authorization` header:

```
Authorization: Bearer YOUR_TOKEN_HERE
```

## Permissions

GoMQTT includes a permission system that controls access to MQTT topics. There are three permission levels:

- **read_only**: Allows subscribing to topics
- **read_write**: Allows subscribing and publishing to topics
- **admin**: Full control over topics

## Role-Based Access Control (RBAC)

If enabled, GoMQTT supports RBAC which allows you to create roles with specific permissions and assign them to users.

## API Structure

The API is organized into the following sections:

- **Authentication**: Login and token management
- **Clients**: Manage and monitor connected clients
- **Messages**: Publish and retrieve messages
- **Users**: User account management
- **Permissions**: Topic permission management
- **Roles**: Role-based access control (if enabled)
- **History**: Message history and retention
- **System**: System status and health checks

## Getting Started

1. Obtain a JWT token by logging in
2. Use the token for all subsequent API requests
3. Explore the available endpoints to manage your MQTT broker

For details on specific endpoints, refer to the API reference.
