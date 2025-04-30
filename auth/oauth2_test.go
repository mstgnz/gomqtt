package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOAuth2Provider_ValidateOAuth2Token(t *testing.T) {
	// Create a mock OAuth2 provider
	provider := &OAuth2Provider{
		ClientID:      "test-client-id",
		ClientSecret:  "test-client-secret",
		AuthURL:       "https://example.com/auth",
		TokenURL:      "https://example.com/token",
		RedirectURL:   "https://example.com/callback",
		UserInfoURL:   "https://example.com/userinfo",
		Scopes:        []string{"email", "profile"},
		TokenField:    "password",
		UsernameField: "email",
	}

	// Set up a test server for handling the user info endpoint
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that the token is in the Authorization header
		token := r.Header.Get("Authorization")
		if token != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Return a valid user info response
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"email": "test@example.com", "name": "Test User"}`))
	}))
	defer ts.Close()

	// Update the provider to use the test server URL
	provider.UserInfoURL = ts.URL

	// Test with a valid token
	username, err := provider.ValidateOAuth2Token("test-token")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if username != "test@example.com" {
		t.Errorf("Expected username to be test@example.com, got %s", username)
	}

	// Test with an invalid token
	username, err = provider.ValidateOAuth2Token("invalid-token")
	if err == nil {
		t.Error("Expected error, got nil")
	}
	if username != "" {
		t.Errorf("Expected empty username, got %s", username)
	}
}

func TestOAuth2Provider_GetAuthorizationURL(t *testing.T) {
	provider := &OAuth2Provider{
		ClientID:    "test-client-id",
		AuthURL:     "https://example.com/auth",
		RedirectURL: "https://example.com/callback",
		Scopes:      []string{"email", "profile"},
	}

	url := provider.GetAuthorizationURL("test-state")
	expected := "https://example.com/auth?response_type=code&client_id=test-client-id&redirect_uri=https://example.com/callback&scope=email profile&state=test-state"
	if url != expected {
		t.Errorf("Expected %s, got %s", expected, url)
	}
}

func TestAuth_AuthenticateOAuth2Token(t *testing.T) {
	// Create a test Auth instance
	authService := New("test-secret")

	// Register a test user
	err := authService.RegisterUser("existing-user", "password", false)
	if err != nil {
		t.Fatalf("Failed to register user: %v", err)
	}

	// Set up a mock OAuth2 provider
	provider := &OAuth2Provider{
		ClientID:      "test-client-id",
		ClientSecret:  "test-client-secret",
		AuthURL:       "https://example.com/auth",
		TokenURL:      "https://example.com/token",
		RedirectURL:   "https://example.com/callback",
		UserInfoURL:   "https://example.com/userinfo",
		Scopes:        []string{"email", "profile"},
		TokenField:    "password",
		UsernameField: "email",
	}

	// Create a mock server for OAuth2 validation
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that the token is in the Authorization header
		token := r.Header.Get("Authorization")
		if token != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Return user info for an existing user
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"email": "existing-user", "name": "Existing User"}`))
	}))
	defer ts.Close()

	// Update the provider to use the test server URL
	provider.UserInfoURL = ts.URL

	// Test with a valid token for an existing user
	username, err := authService.AuthenticateOAuth2Token("test-token", "client1", provider)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if username != "existing-user" {
		t.Errorf("Expected username to be existing-user, got %s", username)
	}

	// Check if the client was registered
	client, err := authService.GetClient("client1")
	if err != nil {
		t.Errorf("Expected client to be registered, got error: %v", err)
	}
	if client.Username != "existing-user" {
		t.Errorf("Expected client username to be existing-user, got %s", client.Username)
	}

	// Set up a server for a new user
	tsNewUser := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token != "Bearer new-user-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Return user info for a new user
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"email": "new-user@example.com", "name": "New User"}`))
	}))
	defer tsNewUser.Close()

	// Update the provider to use the new test server URL
	provider.UserInfoURL = tsNewUser.URL

	// Test with a valid token for a new user
	username, err = authService.AuthenticateOAuth2Token("new-user-token", "client2", provider)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if username != "new-user@example.com" {
		t.Errorf("Expected username to be new-user@example.com, got %s", username)
	}

	// Check if the new user was created in the auth service
	user, err := authService.GetUser("new-user@example.com")
	if err != nil {
		t.Errorf("Expected new user to be created, got error: %v", err)
	}
	if user == nil {
		t.Error("Expected user to be non-nil")
	} else if user.Username != "new-user@example.com" {
		t.Errorf("Expected username to be new-user@example.com, got %s", user.Username)
	}
}
