package auth

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	ErrRoleNotFound     = errors.New("role not found")
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

// Role represents a role for RBAC (Role-Based Access Control)
type Role struct {
	Name        string
	Description string
	Permissions []Permission
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// User represents a user with authentication credentials
type User struct {
	Username    string
	Password    string // Hashed password in production
	APIKeys     []APIKey
	Permissions []Permission // Direct permissions (in addition to role-based permissions)
	Roles       []string     // List of role names assigned to user
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
	roles          map[string]*Role         // role name -> Role
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
		roles:         make(map[string]*Role),
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
		Roles:       []string{},
		CreatedAt:   time.Now(),
	}

	a.users[username] = user
	return nil
}

// RegisterUserWithDefaultRole registers a new user and assigns the default role if provided
func (a *Auth) RegisterUserWithDefaultRole(username, password string, isAdmin bool, defaultRole string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if user already exists
	if _, exists := a.users[username]; exists {
		return errors.New("user already exists")
	}

	// Create roles array, add default role if specified and exists
	roles := []string{}
	if defaultRole != "" {
		if _, exists := a.roles[defaultRole]; exists {
			roles = append(roles, defaultRole)
		}
	}

	// Create a new user
	user := &User{
		Username:    username,
		Password:    password, // In production, this should be hashed
		IsAdmin:     isAdmin,
		Permissions: []Permission{},
		APIKeys:     []APIKey{},
		Roles:       roles,
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

	// Then check user's direct permissions
	for _, perm := range user.Permissions {
		if topicMatches(perm.TopicPattern, topic) {
			if requireWrite && perm.AccessLevel < ReadWrite {
				continue // Need write permission
			}
			return nil // Permission granted
		}
	}

	// Finally, check role-based permissions
	for _, roleName := range user.Roles {
		role, exists := a.roles[roleName]
		if !exists {
			continue
		}

		for _, perm := range role.Permissions {
			if topicMatches(perm.TopicPattern, topic) {
				if requireWrite && perm.AccessLevel < ReadWrite {
					continue // Need write permission
				}
				return nil // Permission granted by role
			}
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

	// Find and remove the matching permission
	for i, perm := range user.Permissions {
		if perm.TopicPattern == topicPattern {
			// Remove permission at index i
			user.Permissions = append(user.Permissions[:i], user.Permissions[i+1:]...)
			return nil
		}
	}

	return errors.New("permission not found")
}

// CreateRole creates a new role
func (a *Auth) CreateRole(name, description string, permissions []Permission) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if role already exists
	if _, exists := a.roles[name]; exists {
		return errors.New("role already exists")
	}

	// Create the role
	role := &Role{
		Name:        name,
		Description: description,
		Permissions: permissions,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	a.roles[name] = role
	return nil
}

// GetRole returns a role by name
func (a *Auth) GetRole(name string) (*Role, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	role, exists := a.roles[name]
	if !exists {
		return nil, ErrRoleNotFound
	}

	return role, nil
}

// UpdateRole updates an existing role
func (a *Auth) UpdateRole(name, description string, permissions []Permission) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	role, exists := a.roles[name]
	if !exists {
		return ErrRoleNotFound
	}

	role.Description = description
	role.Permissions = permissions
	role.UpdatedAt = time.Now()
	return nil
}

// DeleteRole deletes a role and removes it from all users
func (a *Auth) DeleteRole(name string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if _, exists := a.roles[name]; !exists {
		return ErrRoleNotFound
	}

	// Remove this role from all users
	for _, user := range a.users {
		for i, roleName := range user.Roles {
			if roleName == name {
				user.Roles = append(user.Roles[:i], user.Roles[i+1:]...)
				break
			}
		}
	}

	// Delete the role
	delete(a.roles, name)
	return nil
}

// AssignRoleToUser assigns a role to a user
func (a *Auth) AssignRoleToUser(username, roleName string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if user exists
	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Check if role exists
	if _, exists := a.roles[roleName]; !exists {
		return ErrRoleNotFound
	}

	// Check if user already has this role
	for _, r := range user.Roles {
		if r == roleName {
			return nil // User already has this role
		}
	}

	// Assign the role
	user.Roles = append(user.Roles, roleName)
	return nil
}

// RemoveRoleFromUser removes a role from a user
func (a *Auth) RemoveRoleFromUser(username, roleName string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if user exists
	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Find and remove the role
	for i, r := range user.Roles {
		if r == roleName {
			user.Roles = append(user.Roles[:i], user.Roles[i+1:]...)
			return nil
		}
	}

	return errors.New("user does not have this role")
}

// ListRoles returns all roles
func (a *Auth) ListRoles() []*Role {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	roles := make([]*Role, 0, len(a.roles))
	for _, role := range a.roles {
		roles = append(roles, role)
	}
	return roles
}

// GetUserRoles returns all roles assigned to a user
func (a *Auth) GetUserRoles(username string) ([]*Role, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	// Check if user exists
	user, exists := a.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	// Get the roles
	roles := make([]*Role, 0, len(user.Roles))
	for _, roleName := range user.Roles {
		if role, exists := a.roles[roleName]; exists {
			roles = append(roles, role)
		}
	}

	return roles, nil
}

// RoleDefinition represents a role definition in the JSON file
type RoleDefinition struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Permissions []struct {
		TopicPattern string `json:"topic_pattern"`
		AccessLevel  int    `json:"access_level"`
	} `json:"permissions"`
}

// RolesFile represents the structure of the roles JSON file
type RolesFile struct {
	Roles []RoleDefinition `json:"roles"`
}

// LoadRolesFromFile loads roles from a JSON file
func (a *Auth) LoadRolesFromFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read roles file: %w", err)
	}

	var rolesFile RolesFile
	if err := json.Unmarshal(data, &rolesFile); err != nil {
		return fmt.Errorf("failed to parse roles file: %w", err)
	}

	// Lock for writing
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Add each role
	for _, roleDef := range rolesFile.Roles {
		// Convert permissions
		permissions := make([]Permission, len(roleDef.Permissions))
		for i, perm := range roleDef.Permissions {
			permissions[i] = Permission{
				TopicPattern: perm.TopicPattern,
				AccessLevel:  AccessLevel(perm.AccessLevel),
			}

		}

		// Create or update the role
		role, exists := a.roles[roleDef.Name]
		if !exists {
			// Create new role
			a.roles[roleDef.Name] = &Role{
				Name:        roleDef.Name,
				Description: roleDef.Description,
				Permissions: permissions,
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
			}
		} else {
			// Update existing role
			role.Description = roleDef.Description
			role.Permissions = permissions
			role.UpdatedAt = time.Now()
		}
	}

	return nil
}

// ResolveTopicPattern replaces variables in a topic pattern with actual values
func (a *Auth) ResolveTopicPattern(pattern, username, clientID string) string {
	result := strings.Replace(pattern, "{username}", username, -1)
	result = strings.Replace(result, "{client_id}", clientID, -1)
	return result
}

// CheckTopicPermissionWithPatternResolution checks permissions with variable resolution
func (a *Auth) CheckTopicPermissionWithPatternResolution(clientID, topic string, requireWrite bool) error {
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

	// Username for pattern resolution
	username := client.Username

	// Check client permissions first
	for _, perm := range client.Permissions {
		resolvedPattern := a.ResolveTopicPattern(perm.TopicPattern, username, clientID)
		if topicMatches(resolvedPattern, topic) {
			if requireWrite && perm.AccessLevel < ReadWrite {
				continue // Need write permission
			}
			return nil // Permission granted
		}
	}

	// Then check user's direct permissions
	for _, perm := range user.Permissions {
		resolvedPattern := a.ResolveTopicPattern(perm.TopicPattern, username, clientID)
		if topicMatches(resolvedPattern, topic) {
			if requireWrite && perm.AccessLevel < ReadWrite {
				continue // Need write permission
			}
			return nil // Permission granted
		}
	}

	// Finally, check role-based permissions
	for _, roleName := range user.Roles {
		role, exists := a.roles[roleName]
		if !exists {
			continue
		}

		for _, perm := range role.Permissions {
			resolvedPattern := a.ResolveTopicPattern(perm.TopicPattern, username, clientID)
			if topicMatches(resolvedPattern, topic) {
				if requireWrite && perm.AccessLevel < ReadWrite {
					continue // Need write permission
				}
				return nil // Permission granted by role
			}
		}
	}

	return ErrPermissionDenied
}

// GetAllUsers returns all users (for admin use only)
func (a *Auth) GetAllUsers() map[string]*User {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	// Return a copy to ensure thread safety
	usersCopy := make(map[string]*User)
	for username, user := range a.users {
		usersCopy[username] = user
	}
	return usersCopy
}

// DeleteUser deletes a user by username
func (a *Auth) DeleteUser(username string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if _, exists := a.users[username]; !exists {
		return ErrUserNotFound
	}

	// Remove the user
	delete(a.users, username)

	// Also remove any client devices associated with this user
	for clientID, client := range a.clientDevices {
		if client.Username == username {
			delete(a.clientDevices, clientID)
		}
	}

	return nil
}

// IsRBACEnabled indicates if RBAC is enabled
// This should be set by the application based on configuration
var isRBACEnabled bool
var defaultRole string

// SetRBACEnabled sets the RBAC enabled flag
func (a *Auth) SetRBACEnabled(enabled bool) {
	isRBACEnabled = enabled
}

// IsRBACEnabled returns whether RBAC is enabled
func (a *Auth) IsRBACEnabled() bool {
	return isRBACEnabled
}

// SetDefaultRole sets the default role for new users
func (a *Auth) SetDefaultRole(role string) {
	defaultRole = role
}

// GetDefaultRole returns the default role for new users
func (a *Auth) GetDefaultRole() string {
	return defaultRole
}
