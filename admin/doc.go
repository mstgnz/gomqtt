/*
Package admin provides a web-based administration interface for the GoMQTT broker.

The admin panel offers a user-friendly dashboard with the following features:
  - Real-time broker statistics and monitoring
  - Connected client management
  - Message flow visualization
  - Topic explorer
  - User and permission management
  - Configuration settings

The web interface is built using Go templates with HTMX for dynamic content updates
without requiring a JavaScript framework. The admin panel is designed to be lightweight
and responsive, making it suitable for both desktop and mobile browsers.

The admin panel is accessible by default on port 8080 and includes authentication
to prevent unauthorized access to broker management features.
*/
package admin
