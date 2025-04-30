# 🔒 OAuth2 Authentication Guide

GoMQTT supports OAuth2 authentication for secure client connections. This allows integration with popular identity providers like Google, GitHub, Auth0, and others.

## OAuth2 Overview

OAuth2 is an industry-standard protocol for authorization that enables third-party applications to obtain limited access to a user's account on a server without exposing their credentials. GoMQTT implements OAuth2 to allow clients to authenticate using tokens issued by trusted identity providers.

## Configuring OAuth2 in GoMQTT

To enable OAuth2 authentication, update your configuration file:

```json
{
  "auth": {
    "jwt_secret": "your-jwt-secret",
    "jwt_expires": 24,
    "oauth2": {
      "enabled": true,
      "client_id": "your-client-id",
      "client_secret": "your-client-secret",
      "auth_url": "https://accounts.google.com/o/oauth2/auth",
      "token_url": "https://oauth2.googleapis.com/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["email", "profile"],
      "user_info_url": "https://www.googleapis.com/oauth2/v3/userinfo",
      "token_field": "password",
      "username_field": "email"
    }
  }
}
```

### Configuration Options

- `enabled`: Set to `true` to enable OAuth2 authentication
- `client_id`: Client ID from your OAuth2 provider
- `client_secret`: Client secret from your OAuth2 provider
- `auth_url`: Authorization URL for the OAuth2 provider
- `token_url`: Token URL for the OAuth2 provider
- `redirect_url`: Callback URL for the OAuth2 flow
- `scopes`: OAuth2 scopes to request
- `user_info_url`: URL to retrieve user information
- `token_field`: Field in MQTT CONNECT packet to use for the token (usually "password")
- `username_field`: Field in user info response to use as username

## Setting Up OAuth2 Providers

### Google

1. Create a project at [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to "APIs & Services" > "Credentials"
3. Create an OAuth client ID (Web application)
4. Configure your redirect URI: `http://localhost:8080/oauth/callback`
5. Use the following configuration:

```json
{
  "auth": {
    "oauth2": {
      "enabled": true,
      "client_id": "your-google-client-id",
      "client_secret": "your-google-client-secret",
      "auth_url": "https://accounts.google.com/o/oauth2/auth",
      "token_url": "https://oauth2.googleapis.com/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["email", "profile"],
      "user_info_url": "https://www.googleapis.com/oauth2/v3/userinfo",
      "token_field": "password",
      "username_field": "email"
    }
  }
}
```

### GitHub

1. Register a new OAuth application at [GitHub Developer Settings](https://github.com/settings/developers)
2. Configure your redirect URI: `http://localhost:8080/oauth/callback`
3. Use the following configuration:

```json
{
  "auth": {
    "oauth2": {
      "enabled": true,
      "client_id": "your-github-client-id",
      "client_secret": "your-github-client-secret",
      "auth_url": "https://github.com/login/oauth/authorize",
      "token_url": "https://github.com/login/oauth/access_token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["user:email"],
      "user_info_url": "https://api.github.com/user",
      "token_field": "password",
      "username_field": "login"
    }
  }
}
```

### Auth0

1. Create an application in your [Auth0 Dashboard](https://manage.auth0.com/)
2. Configure your callback URL: `http://localhost:8080/oauth/callback`
3. Use the following configuration:

```json
{
  "auth": {
    "oauth2": {
      "enabled": true,
      "client_id": "your-auth0-client-id",
      "client_secret": "your-auth0-client-secret",
      "auth_url": "https://your-tenant.auth0.com/authorize",
      "token_url": "https://your-tenant.auth0.com/oauth/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["openid", "profile", "email"],
      "user_info_url": "https://your-tenant.auth0.com/userinfo",
      "token_field": "password",
      "username_field": "email"
    }
  }
}
```

### Okta

1. Register a new application in the [Okta Developer Dashboard](https://developer.okta.com/)
2. Set up the redirect URI: `http://localhost:8080/oauth/callback`
3. Use the following configuration:

```json
{
  "auth": {
    "oauth2": {
      "enabled": true,
      "client_id": "your-okta-client-id",
      "client_secret": "your-okta-client-secret",
      "auth_url": "https://your-org.okta.com/oauth2/default/v1/authorize",
      "token_url": "https://your-org.okta.com/oauth2/default/v1/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["openid", "profile", "email"],
      "user_info_url": "https://your-org.okta.com/oauth2/default/v1/userinfo",
      "token_field": "password",
      "username_field": "email"
    }
  }
}
```

### Azure AD

1. Register an application in the [Azure Portal](https://portal.azure.com/)
2. Configure redirect URI: `http://localhost:8080/oauth/callback`
3. Use the following configuration:

```json
{
  "auth": {
    "oauth2": {
      "enabled": true,
      "client_id": "your-azure-client-id",
      "client_secret": "your-azure-client-secret",
      "auth_url": "https://login.microsoftonline.com/your-tenant-id/oauth2/v2.0/authorize",
      "token_url": "https://login.microsoftonline.com/your-tenant-id/oauth2/v2.0/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["openid", "profile", "email", "User.Read"],
      "user_info_url": "https://graph.microsoft.com/v1.0/me",
      "token_field": "password",
      "username_field": "userPrincipalName"
    }
  }
}
```

## Client Connection with OAuth2

When connecting with an MQTT client, use:

- Username: The username field from your OAuth2 provider (usually email)
- Password: The OAuth2 token received from your provider

## MQTT Client Examples

### Python Example

```python
import paho.mqtt.client as mqtt
import requests
import json
import webbrowser

# First, open the browser to get the OAuth2 authorization code
auth_url = "https://accounts.google.com/o/oauth2/auth?response_type=code&client_id=your-client-id&redirect_uri=http://localhost:8080/oauth/callback&scope=email profile"
webbrowser.open(auth_url)

# After authorization, you'll get a code in the callback URL
# Enter that code here:
code = input("Enter the authorization code from the callback URL: ")

# Exchange the code for a token
token_response = requests.post(
    "https://oauth2.googleapis.com/token",
    data={
        "code": code,
        "client_id": "your-client-id",
        "client_secret": "your-client-secret",
        "redirect_uri": "http://localhost:8080/oauth/callback",
        "grant_type": "authorization_code"
    }
)

token_data = token_response.json()
access_token = token_data["access_token"]

# Get user info to determine username
user_info = requests.get(
    "https://www.googleapis.com/oauth2/v3/userinfo",
    headers={"Authorization": f"Bearer {access_token}"}
)
user_data = user_info.json()
email = user_data["email"]

# Connect to MQTT broker with OAuth2 token
client = mqtt.Client()
client.username_pw_set(email, password=access_token)
client.connect("localhost", 1883, 60)

# Now you can publish/subscribe
client.publish("test/topic", "Hello with OAuth2!")
client.subscribe("test/topic")

# Start the loop
client.loop_forever()
```

### JavaScript/Node.js Example

```javascript
const mqtt = require("mqtt");
const axios = require("axios");
const open = require("open");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

// Configuration
const config = {
  clientId: "your-client-id",
  clientSecret: "your-client-secret",
  redirectUri: "http://localhost:8080/oauth/callback",
  authUrl: "https://accounts.google.com/o/oauth2/auth",
  tokenUrl: "https://oauth2.googleapis.com/token",
  userInfoUrl: "https://www.googleapis.com/oauth2/v3/userinfo",
  scope: "email profile",
};

// Open browser for authorization
const authUrl = `${config.authUrl}?response_type=code&client_id=${config.clientId}&redirect_uri=${config.redirectUri}&scope=${config.scope}`;
open(authUrl);

// Get authorization code from user
rl.question(
  "Enter the authorization code from the callback URL: ",
  async (code) => {
    try {
      // Exchange code for token
      const tokenResponse = await axios.post(config.tokenUrl, null, {
        params: {
          code,
          client_id: config.clientId,
          client_secret: config.clientSecret,
          redirect_uri: config.redirectUri,
          grant_type: "authorization_code",
        },
      });

      const accessToken = tokenResponse.data.access_token;

      // Get user info
      const userInfoResponse = await axios.get(config.userInfoUrl, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const email = userInfoResponse.data.email;

      // Connect to MQTT broker with OAuth2 credentials
      const client = mqtt.connect("mqtt://localhost:1883", {
        username: email,
        password: accessToken,
      });

      client.on("connect", () => {
        console.log("Connected to MQTT broker!");

        // Subscribe to a topic
        client.subscribe("test/topic", (err) => {
          if (!err) {
            // Publish a message
            client.publish("test/topic", "Hello with OAuth2!");
          }
        });
      });

      client.on("message", (topic, message) => {
        console.log(`Received message on ${topic}: ${message.toString()}`);
      });
    } catch (error) {
      console.error("Error:", error.response?.data || error.message);
    } finally {
      rl.close();
    }
  }
);
```

## Testing OAuth2 Setup

You can test your OAuth2 setup with a simple command line tool:

```bash
# Install a command line OAuth2 client
go install github.com/cli/oauth2-helper@latest

# Get an OAuth2 token (this will open a browser)
oauth2-helper \
  --client-id YOUR_CLIENT_ID \
  --client-secret YOUR_CLIENT_SECRET \
  --scopes "email profile" \
  --auth-url "https://accounts.google.com/o/oauth2/auth" \
  --token-url "https://oauth2.googleapis.com/token"

# The tool will return an access token which you can use with MQTT clients
```

## Troubleshooting

### Common Issues

1. **Invalid Redirect URI**: Make sure the redirect URI in your provider settings exactly matches the one in your GoMQTT configuration.

2. **Incorrect Scopes**: The OAuth2 provider may require specific scopes to access user information.

3. **Token Expiration**: OAuth2 tokens expire. Implement a token refresh mechanism in long-running clients.

4. **CORS Issues**: If using browser-based clients, ensure CORS is properly configured.

5. **Transport Security**: Some OAuth2 providers require HTTPS for the redirect URL in production environments.

### Debugging

To enable OAuth2 debugging, set the log level to "debug" in your configuration:

```json
{
  "logging": {
    "level": "debug"
  }
}
```

This will output detailed logs about the OAuth2 authentication process.
