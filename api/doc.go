/*
Package api provides a RESTful HTTP API for the GoMQTT broker.

It enables programmatic access to broker functionality including:
  - Client management (listing clients, client details)
  - User and role management with RBAC
  - Topic permission control
  - Message history and monitoring
  - Topic management
  - Broker configuration

The API follows REST principles and uses JWT authentication for secure access.
All API endpoints return JSON responses and accept JSON for request bodies.

The API server also provides OpenAPI documentation through Scalar UI at the root endpoint.
*/
package api
