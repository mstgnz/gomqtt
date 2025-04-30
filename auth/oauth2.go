package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// OAuth2Provider manages OAuth2 authentication
type OAuth2Provider struct {
	ClientID      string
	ClientSecret  string
	AuthURL       string
	TokenURL      string
	RedirectURL   string
	UserInfoURL   string
	Scopes        []string
	TokenField    string
	UsernameField string
}

// OAuth2Config represents OAuth2 configuration settings
type OAuth2Config struct {
	Enabled       bool
	ClientID      string
	ClientSecret  string
	AuthURL       string
	TokenURL      string
	RedirectURL   string
	Scopes        []string
	UserInfoURL   string
	TokenField    string
	UsernameField string
}

// NewOAuth2Provider creates a new OAuth2 authentication provider
func NewOAuth2Provider(config OAuth2Config) *OAuth2Provider {
	return &OAuth2Provider{
		ClientID:      config.ClientID,
		ClientSecret:  config.ClientSecret,
		AuthURL:       config.AuthURL,
		TokenURL:      config.TokenURL,
		RedirectURL:   config.RedirectURL,
		Scopes:        config.Scopes,
		UserInfoURL:   config.UserInfoURL,
		TokenField:    config.TokenField,
		UsernameField: config.UsernameField,
	}
}

// ValidateOAuth2Token validates an OAuth2 token and returns user information
func (p *OAuth2Provider) ValidateOAuth2Token(token string) (string, error) {
	// Set a timeout for the request
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a request to the user info endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.UserInfoURL, nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %w", err)
	}

	// Add the OAuth2 token to the Authorization header
	req.Header.Add("Authorization", "Bearer "+token)

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error validating token: %w", err)
	}
	defer resp.Body.Close()

	// Check if the request was successful
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid token, status code: %d", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %w", err)
	}

	// Parse the JSON response
	var userInfo map[string]interface{}
	if err := json.Unmarshal(body, &userInfo); err != nil {
		return "", fmt.Errorf("error parsing user info: %w", err)
	}

	// Extract the username
	usernameRaw, ok := userInfo[p.UsernameField]
	if !ok {
		return "", fmt.Errorf("username field '%s' not found in user info", p.UsernameField)
	}

	username, ok := usernameRaw.(string)
	if !ok {
		return "", errors.New("username is not a string")
	}

	return username, nil
}

// GetAuthorizationURL returns the URL to redirect the user for OAuth2 authorization
func (p *OAuth2Provider) GetAuthorizationURL(state string) string {
	// Build the scopes string
	scopeStr := ""
	for i, scope := range p.Scopes {
		if i > 0 {
			scopeStr += " "
		}
		scopeStr += scope
	}

	// Construct the authorization URL
	return fmt.Sprintf("%s?response_type=code&client_id=%s&redirect_uri=%s&scope=%s&state=%s",
		p.AuthURL,
		p.ClientID,
		p.RedirectURL,
		scopeStr,
		state)
}

// ExchangeCodeForToken exchanges an authorization code for an access token
func (p *OAuth2Provider) ExchangeCodeForToken(code string) (string, error) {
	// Set up the request to exchange code for token
	client := &http.Client{Timeout: 10 * time.Second}

	// Prepare the request body
	reqURL := fmt.Sprintf("%s?grant_type=authorization_code&code=%s&redirect_uri=%s&client_id=%s&client_secret=%s",
		p.TokenURL,
		code,
		p.RedirectURL,
		p.ClientID,
		p.ClientSecret)

	// Send the request
	req, err := http.NewRequest(http.MethodPost, reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("error creating token request: %w", err)
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error exchanging code for token: %w", err)
	}
	defer resp.Body.Close()

	// Check the response
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("error exchanging code for token, status code: %d", resp.StatusCode)
	}

	// Parse the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading token response: %w", err)
	}

	var tokenResp map[string]interface{}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("error parsing token response: %w", err)
	}

	// Extract the access token
	accessToken, ok := tokenResp["access_token"].(string)
	if !ok {
		return "", errors.New("access_token not found in response")
	}

	return accessToken, nil
}

// GenerateOAuth2Token creates a JWT token for the user authenticated via OAuth2
func (a *Auth) GenerateOAuth2Token(username, clientID string, expiration time.Duration) (string, error) {
	claims := &Claims{
		ClientID: clientID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(expiration)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "gomqtt",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(a.secretKey)
}

// AuthenticateOAuth2Token authenticates a user with an OAuth2 token
func (a *Auth) AuthenticateOAuth2Token(token, clientID string, provider *OAuth2Provider) (string, error) {
	// Validate the OAuth2 token
	username, err := provider.ValidateOAuth2Token(token)
	if err != nil {
		return "", err
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if the user exists
	user, exists := a.users[username]
	if !exists {
		// Create a new user if not found
		user = &User{
			Username:    username,
			Password:    "", // No password for OAuth2 users
			Permissions: []Permission{},
			APIKeys:     []APIKey{},
			IsAdmin:     false,
			CreatedAt:   time.Now(),
			LastLogin:   time.Now(),
		}
		a.users[username] = user
	} else {
		// Update last login time
		user.LastLogin = time.Now()
	}

	// Register the client if not already registered
	if _, err := a.GetClient(clientID); err == ErrClientNotFound {
		if err := a.RegisterClient(clientID, username); err != nil {
			return "", err
		}
	}

	return username, nil
}
