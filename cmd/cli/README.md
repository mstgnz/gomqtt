# GoMQTT CLI

A comprehensive command-line interface for managing and monitoring the GoMQTT broker.

## Installation

To build the CLI tool:

```bash
go build -o gomqtt-cli github.com/mstgnz/gomqtt/cmd/cli
```

## Usage

The CLI provides various commands to manage different aspects of the GoMQTT broker:

```
GoMQTT CLI provides tools for managing and monitoring your MQTT broker.
Complete documentation is available at https://github.com/mstgnz/gomqtt

Usage:
  gomqtt-cli [command]

Available Commands:
  client      Manage MQTT clients
  cluster     Manage cluster
  config      Manage configuration
  help        Help about any command
  metrics     Show broker metrics
  start       Start the MQTT broker
  status      Show broker status
  topic       Manage MQTT topics
  user        Manage users

Flags:
  -c, --config string   Configuration file path (default "config/default.json")
  -h, --help            help for gomqtt-cli

Use "gomqtt-cli [command] --help" for more information about a command.
```

### Configuration

All commands use the configuration file specified with the `-c` or `--config` flag:

```bash
gomqtt-cli -c /path/to/config.json [command]
```

### Client Management

List connected clients:

```bash
gomqtt-cli client list
```

Disconnect a client:

```bash
gomqtt-cli client disconnect client-123
```

### Topic Management

List active topics:

```bash
gomqtt-cli topic list
```

Publish a message to a topic:

```bash
gomqtt-cli topic publish "sensors/temperature" "25.5" -q 1 -r
```

Options:

- `-q, --qos` - QoS level (0, 1, or 2)
- `-r, --retain` - Set retain flag

### User Management

Create a new user:

```bash
gomqtt-cli user create username password -r admin
```

Options:

- `-r, --role` - Role for the new user (default "user")

List users:

```bash
gomqtt-cli user list
```

### Configuration Management

Show current configuration:

```bash
gomqtt-cli config show
```

Show configuration in JSON format:

```bash
gomqtt-cli config show -f json
```

### Cluster Management

Show cluster status:

```bash
gomqtt-cli cluster status
```

### Metrics

Show broker metrics:

```bash
gomqtt-cli metrics
```

## Development Status

This CLI tool is currently under development. Some features require additional implementation in the GoMQTT core modules:

1. **Storage Interface Extensions**:

   - Methods for retrieving connected clients
   - Methods for listing active topics

2. **Auth Interface Extensions**:

   - Methods for user management (create, list)

3. **API Integration**:
   - For runtime operations like disconnecting clients
   - Publishing messages
   - Retrieving real-time metrics

## Contributing

Contributions to extend the CLI functionality are welcome! Please see the main GoMQTT project for contribution guidelines.
