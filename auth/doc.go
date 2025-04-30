/*
Package auth provides authentication and authorization mechanisms for GoMQTT.

This package implements various authentication methods and access control systems:

  - JWT-based authentication
  - OAuth2 integration
  - Username/password verification
  - Client certificate validation (mTLS)
  - Role-Based Access Control (RBAC)
  - Topic permission management

# Authentication Service

The Auth struct provides authentication and authorization services:

	// Create a new auth service with JWT support
	authService := auth.New("your-jwt-secret")

	// Authenticate a client
	authenticated, username, err := authService.Authenticate("clientID", "username", "password")
	if err != nil {
	    // Handle error
	}
	if !authenticated {
	    // Authentication failed
	}

	// Check topic permissions
	if err := authService.CheckTopicPermission(username, "sensors/temp", true); err != nil {
	    // Permission denied
	}

# JWT Authentication

JWT tokens can be used for client authentication:

  - Token generation and validation
  - Expiration time management
  - Claims verification
  - User role assignment

# OAuth2 Integration

OAuth2 authentication allows integration with identity providers:

	// Configure OAuth2 provider
	oauth2Provider := auth.NewOAuth2Provider(auth.OAuth2Config{
	    Enabled:       true,
	    ClientID:      "your-client-id",
	    ClientSecret:  "your-client-secret",
	    AuthURL:       "https://accounts.google.com/o/oauth2/auth",
	    TokenURL:      "https://oauth2.googleapis.com/token",
	    RedirectURL:   "http://localhost:8080/oauth/callback",
	    Scopes:        []string{"email", "profile"},
	    UserInfoURL:   "https://www.googleapis.com/oauth2/v3/userinfo",
	    TokenField:    "password",
	    UsernameField: "email",
	})

	// Add to auth service
	authService.SetOAuth2Provider(oauth2Provider)

# Role-Based Access Control

RBAC provides fine-grained permission management:

	// Enable RBAC
	authService.SetRBACEnabled(true)

	// Set default role for new users
	authService.SetDefaultRole("user")

	// Create a role with permissions
	adminPermissions := []auth.Permission{
	    {
	        TopicPattern: "#",
	        AccessLevel:  auth.ReadWrite,
	    },
	}

	authService.CreateRole("admin", "Administrator role", adminPermissions)

	// Assign role to user
	authService.AssignRole("username", "admin")

# Topic Permissions

Topic permission control is based on pattern matching:

  - Wildcard support (+ and # characters)
  - Variable substitution (e.g., "user/{username}/#")
  - Read/write permission levels
  - Role inheritance

# Client Certificate Verification

For TLS connections, client certificates can be validated:

  - Certificate chain verification
  - Common Name (CN) extraction for username
  - Expiration validation
  - Certificate revocation checking

# Examples

Basic authentication setup:

	// Create auth service
	authService := auth.New("jwt-secret")

	// Set up a basic authenticator
	authService.SetBasicAuthenticator(func(username, password string) bool {
	    // Implement your authentication logic here
	    return username == "admin" && password == "secure123"
	})

	// Add to MQTT server
	mqttServer.SetAuthService(authService)

RBAC with topic permissions:

	// Enable RBAC
	authService.SetRBACEnabled(true)

	// Create roles
	userPermissions := []auth.Permission{
	    {
	        TopicPattern: "user/{username}/#",
	        AccessLevel:  auth.ReadWrite,
	    },
	    {
	        TopicPattern: "public/#",
	        AccessLevel:  auth.ReadOnly,
	    },
	}

	authService.CreateRole("user", "Standard user", userPermissions)

	// Check permissions
	err := authService.CheckTopicPermission("john", "user/john/data", true)
	// err will be nil since john has write access to user/john/#

	err = authService.CheckTopicPermission("john", "user/mary/data", true)
	// err will be non-nil since john doesn't have access to user/mary/#
*/
package auth
