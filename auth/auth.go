package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken     = errors.New("invalid token")
	ErrExpiredToken     = errors.New("token expired")
	ErrInvalidAPIKey    = errors.New("invalid api key")
	ErrUserNotFound     = errors.New("user not found")
	ErrClientNotFound   = errors.New("client not found")
	ErrPermissionDenied = errors.New("permission denied")
)

// AccessLevel defines the permission level
type AccessLevel int

// Permission levels
const (
	ReadOnly AccessLevel = iota
	ReadWrite
	Admin
)

// Permission defines access control for topics
type Permission struct {
	TopicPattern string // Can include wildcards (* and #)
	AccessLevel  AccessLevel
}

// User represents a user with authentication credentials
type User struct {
	Username    string
	Password    string // Hashed password in production
	APIKeys     []APIKey
	Permissions []Permission
	IsAdmin     bool
	CreatedAt   time.Time
	LastLogin   time.Time
}

// APIKey represents an API key with permissions
type APIKey struct {
	Key         string
	Description string
	Permissions []Permission
	CreatedAt   time.Time
	ExpiresAt   time.Time
	LastUsed    time.Time
}

// ClientDevice represents a connected MQTT client
type ClientDevice struct {
	ClientID    string
	Username    string
	Permissions []Permission
	LastConnect time.Time
	IsBlocked   bool
}

// Auth represents the authentication service
type Auth struct {
	secretKey      []byte
	users          map[string]*User         // username -> User
	apiKeys        map[string]*APIKey       // api key -> APIKey
	clientDevices  map[string]*ClientDevice // clientID -> ClientDevice
	oauth2Provider *OAuth2Provider          // OAuth2 provider if enabled
	mutex          sync.RWMutex
}

// New creates a new authentication service
func New(secretKey string) *Auth {
	return &Auth{
		secretKey:     []byte(secretKey),
		users:         make(map[string]*User),
		apiKeys:       make(map[string]*APIKey),
		clientDevices: make(map[string]*ClientDevice),
	}
}

// SetOAuth2Provider sets the OAuth2 provider for authentication
func (a *Auth) SetOAuth2Provider(provider *OAuth2Provider) {
	a.oauth2Provider = provider
}

// Claims represents the JWT claims structure
type Claims struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// GenerateToken creates a new JWT token for the given client
func (a *Auth) GenerateToken(clientID, username string, expiration time.Duration) (string, error) {
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

// ValidateToken validates the given JWT token
func (a *Auth) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidToken
		}
		return a.secretKey, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}

	return nil, ErrInvalidToken
}

// GenerateAPIKey generates a new API key
func (a *Auth) GenerateAPIKey(length int) (string, error) {
	if length <= 0 {
		length = 32 // Default length
	}

	buffer := make([]byte, length)
	_, err := rand.Read(buffer)
	if err != nil {
		return "", err
	}

	// Encode to base64 and remove non-alphanumeric characters
	key := base64.StdEncoding.EncodeToString(buffer)
	key = strings.ReplaceAll(key, "/", "")
	key = strings.ReplaceAll(key, "+", "")
	key = strings.ReplaceAll(key, "=", "")

	// Trim to requested length
	if len(key) > length {
		key = key[:length]
	}

	return key, nil
}

// CreateAPIKey creates and registers a new API key for a user
func (a *Auth) CreateAPIKey(username, description string, permissions []Permission, expiration time.Duration) (string, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if user exists
	user, exists := a.users[username]
	if !exists {
		return "", ErrUserNotFound
	}

	// Generate a new API key
	key, err := a.GenerateAPIKey(32)
	if err != nil {
		return "", err
	}

	// Create API key object
	apiKey := &APIKey{
		Key:         key,
		Description: description,
		Permissions: permissions,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(expiration),
	}

	// Register the API key
	a.apiKeys[key] = apiKey
	user.APIKeys = append(user.APIKeys, *apiKey)

	return key, nil
}

// ValidateAPIKey validates the given API key
func (a *Auth) ValidateAPIKey(apiKey string) (*APIKey, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	key, exists := a.apiKeys[apiKey]
	if !exists {
		return nil, ErrInvalidAPIKey
	}

	// Check if the key has expired
	if !key.ExpiresAt.IsZero() && time.Now().After(key.ExpiresAt) {
		return nil, ErrExpiredToken
	}

	// Update last used time
	key.LastUsed = time.Now()

	return key, nil
}

// RevokeAPIKey revokes an API key
func (a *Auth) RevokeAPIKey(apiKey string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if the API key exists
	if _, exists := a.apiKeys[apiKey]; !exists {
		return ErrInvalidAPIKey
	}

	// Remove the API key
	delete(a.apiKeys, apiKey)

	// Also remove from user's API keys
	for _, user := range a.users {
		for i, key := range user.APIKeys {
			if key.Key == apiKey {
				// Remove this key
				user.APIKeys = append(user.APIKeys[:i], user.APIKeys[i+1:]...)
				break
			}
		}
	}

	return nil
}

// RegisterUser registers a new user
func (a *Auth) RegisterUser(username, password string, isAdmin bool) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if user already exists
	if _, exists := a.users[username]; exists {
		return errors.New("user already exists")
	}

	// Create a new user
	user := &User{
		Username:    username,
		Password:    password, // In production, this should be hashed
		IsAdmin:     isAdmin,
		Permissions: []Permission{},
		APIKeys:     []APIKey{},
		CreatedAt:   time.Now(),
	}

	a.users[username] = user
	return nil
}

// AuthenticateUser authenticates a user
func (a *Auth) AuthenticateUser(username, password string) (*User, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	// Check if the user exists
	user, exists := a.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	// OAuth2 Case: If OAuth2 is enabled and password is being used as token field
	if a.oauth2Provider != nil && a.oauth2Provider.TokenField == "password" {
		// Try to authenticate with OAuth2
		if password != "" {
			if _, err := a.oauth2Provider.ValidateOAuth2Token(password); err == nil {
				// Update last login time
				user.LastLogin = time.Now()
				return user, nil
			}
		}
	}

	// Regular user/password authentication
	if user.Password != password {
		return nil, errors.New("invalid credentials")
	}

	// Update last login time
	user.LastLogin = time.Now()

	return user, nil
}

// RegisterClient registers a client device
func (a *Auth) RegisterClient(clientID, username string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if user exists
	if _, exists := a.users[username]; !exists {
		return ErrUserNotFound
	}

	// Create or update client device
	a.clientDevices[clientID] = &ClientDevice{
		ClientID:    clientID,
		Username:    username,
		Permissions: []Permission{},
		LastConnect: time.Now(),
		IsBlocked:   false,
	}

	return nil
}

// GetClient gets a client device by ID
func (a *Auth) GetClient(clientID string) (*ClientDevice, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	client, exists := a.clientDevices[clientID]
	if !exists {
		return nil, ErrClientNotFound
	}

	return client, nil
}

// BlockClient blocks a client from connecting
func (a *Auth) BlockClient(clientID string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	client, exists := a.clientDevices[clientID]
	if !exists {
		return ErrClientNotFound
	}

	client.IsBlocked = true
	return nil
}

// UnblockClient unblocks a client
func (a *Auth) UnblockClient(clientID string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	client, exists := a.clientDevices[clientID]
	if !exists {
		return ErrClientNotFound
	}

	client.IsBlocked = false
	return nil
}

// AddUserPermission adds a permission to a user
func (a *Auth) AddUserPermission(username, topicPattern string, accessLevel AccessLevel) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	permission := Permission{
		TopicPattern: topicPattern,
		AccessLevel:  accessLevel,
	}

	user.Permissions = append(user.Permissions, permission)
	return nil
}

// CheckTopicPermission checks if a client has permission for a topic
func (a *Auth) CheckTopicPermission(clientID, topic string, requireWrite bool) error {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	client, exists := a.clientDevices[clientID]
	if !exists {
		return ErrClientNotFound
	}

	if client.IsBlocked {
		return ErrPermissionDenied
	}

	// Get user permissions
	user, exists := a.users[client.Username]
	if !exists {
		return ErrUserNotFound
	}

	// Admin has all permissions
	if user.IsAdmin {
		return nil
	}

	// Check client permissions first
	for _, perm := range client.Permissions {
		if topicMatches(perm.TopicPattern, topic) {
			if requireWrite && perm.AccessLevel < ReadWrite {
				continue // Need write permission
			}
			return nil // Permission granted
		}
	}

	// Then check user permissions
	for _, perm := range user.Permissions {
		if topicMatches(perm.TopicPattern, topic) {
			if requireWrite && perm.AccessLevel < ReadWrite {
				continue // Need write permission
			}
			return nil // Permission granted
		}
	}

	return ErrPermissionDenied
}

// topicMatches checks if a topic matches a pattern (with wildcards)
func topicMatches(pattern, topic string) bool {
	// Exact match
	if pattern == topic {
		return true
	}

	// Multi-level wildcard at the end
	if pattern == "#" {
		return true
	}

	patternParts := strings.Split(pattern, "/")
	topicParts := strings.Split(topic, "/")

	// If pattern ends with #, it matches any topic that starts with the pattern
	if patternParts[len(patternParts)-1] == "#" {
		// Check that topic is at least as long as pattern without the #
		if len(topicParts) < len(patternParts)-1 {
			return false
		}

		// Check each segment until the #
		for i := range patternParts[:len(patternParts)-1] {
			if patternParts[i] != "+" && patternParts[i] != topicParts[i] {
				return false
			}
		}
		return true
	}

	// Single level wildcards (+)
	if len(patternParts) != len(topicParts) {
		return false
	}

	for i := range patternParts {
		if patternParts[i] != "+" && patternParts[i] != topicParts[i] {
			return false
		}
	}

	return true
}

// GetUser returns a user by username
func (a *Auth) GetUser(username string) (*User, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	user, exists := a.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	return user, nil
}

// RemoveUserPermission removes a permission for a user
func (a *Auth) RemoveUserPermission(username, topicPattern string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Find the user
	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Find and remove the permission
	for i, perm := range user.Permissions {
		if perm.TopicPattern == topicPattern {
			// Remove this permission
			user.Permissions = append(user.Permissions[:i], user.Permissions[i+1:]...)
			return nil
		}
	}

	return errors.New("permission not found")
}
