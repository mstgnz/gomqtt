# DoS Protection Plugin

This plugin provides advanced Denial of Service (DoS) protection features beyond the standard rate limiting, offering robust protection against various types of attacks targeting MQTT brokers.

## Features

- **IP-based Tracking**: Monitor connection patterns by IP address to detect and block malicious traffic
- **Progressive Penalties**: Apply escalating ban durations for repeat offenders
- **Connection Flood Protection**: Detect and prevent connection flooding attacks
- **Global Rate Limiting**: Apply broker-wide connection rate limits to prevent distributed attacks
- **Client and IP Banning**: Temporarily ban clients and IPs that violate security policies
- **Whitelist Support**: Exempt trusted IPs from DoS protection
- **Memory Protection**: Efficient cleanup of tracking data to prevent memory leaks
- **Low Performance Impact**: Designed for minimal overhead on broker performance

## Configuration

The DoS protection plugin is configured through the following settings:

```json
{
  "plugins": {
    "dos_protection": {
      "enabled": true,
      "connection_rate": 20,
      "connection_burst": 30,
      "publish_rate": 100,
      "subscribe_rate": 30,
      "byte_rate": 1048576,
      "window_size": 60,
      "ip_whitelist": ["127.0.0.1", "192.168.0.0/24"],
      "max_connections_per_ip": 10,
      "temporary_ban_duration": "5m",
      "failed_auth_threshold": 3,
      "connection_flood_interval": "10s",
      "connection_flood_count": 30,
      "global_connection_rate": 200,
      "progressive_ban_enabled": true,
      "max_ban_duration": "24h",
      "enable_logging": true
    }
  }
}
```

### Configuration Parameters

| Parameter                   | Type     | Description                                          | Default       |
| --------------------------- | -------- | ---------------------------------------------------- | ------------- |
| `connection_rate`           | int      | Maximum connection attempts per IP in window         | 20            |
| `connection_burst`          | int      | Maximum burst of connections allowed                 | 30            |
| `publish_rate`              | int      | Maximum publish messages per client in window        | 100           |
| `subscribe_rate`            | int      | Maximum subscribe requests per client in window      | 30            |
| `byte_rate`                 | int      | Maximum bytes per client in window                   | 1048576 (1MB) |
| `window_size`               | int      | Time window in seconds for rate counting             | 60            |
| `ip_whitelist`              | []string | IP addresses/CIDR ranges exempt from protection      | []            |
| `max_connections_per_ip`    | int      | Maximum concurrent connections per IP                | 10            |
| `temporary_ban_duration`    | string   | Duration for temporary bans                          | "5m"          |
| `failed_auth_threshold`     | int      | Failed authentication attempts before ban            | 3             |
| `connection_flood_interval` | string   | Time window for connection flood detection           | "10s"         |
| `connection_flood_count`    | int      | Connections in interval to trigger flood protection  | 30            |
| `global_connection_rate`    | int      | Global limit for connections across all IPs          | 200           |
| `progressive_ban_enabled`   | bool     | Enable escalating ban durations for repeat offenders | true          |
| `max_ban_duration`          | string   | Maximum ban duration                                 | "24h"         |
| `enable_logging`            | bool     | Enable detailed DoS protection logging               | true          |

## Use Cases

### Protection Against Connection Flooding

The plugin detects rapid connection attempts from a single IP and automatically bans the source IP when it exceeds configured thresholds.

### Defending Against Distributed Attacks

Global rate limiting prevents the broker from being overwhelmed even when attacks come from multiple IPs.

### Preserving Service for Legitimate Clients

By banning malicious clients, the broker remains available for legitimate users during attack conditions.

### Preventing Resource Exhaustion

Limiting connections per IP helps prevent memory exhaustion attacks where an attacker tries to open many connections from a single source.

## Integration with Rate Limiting

The DoS protection plugin extends the functionality of the basic rate limiting plugin by adding:

1. IP-based tracking and rate limiting (not just client-ID based)
2. Temporary banning capability with progressive penalties
3. Global connection rate monitoring
4. Connection flood detection
5. Behavioral pattern analysis for smarter protection

## Monitoring and Logs

When logging is enabled (`enable_logging: true`), the plugin logs detailed information about blocked attacks, including:

- Connection flood events
- Temporary IP and client bans
- Rate limit violations
- Ban expiry events

These logs can be monitored to understand attack patterns and adjust configuration if needed.

## Best Practices

1. **Start with Conservative Limits**: Begin with higher limits and gradually reduce them to find the right balance
2. **Whitelist Trusted IPs**: Always whitelist internal services and administrative IPs
3. **Enable Progressive Banning**: This provides stronger protection against persistent attackers
4. **Adjust Window Size**: For high-traffic environments, a larger window size might be appropriate
5. **Monitor Logs**: Regularly review logs to tune the configuration based on actual traffic patterns

## Example Setup

For a typical IoT deployment with moderate traffic:

```json
{
  "plugins": {
    "dos_protection": {
      "enabled": true,
      "connection_rate": 15,
      "publish_rate": 50,
      "max_connections_per_ip": 5,
      "connection_flood_interval": "15s",
      "connection_flood_count": 20,
      "progressive_ban_enabled": true,
      "ip_whitelist": ["10.0.0.0/8", "192.168.0.0/16"]
    }
  }
}
```
