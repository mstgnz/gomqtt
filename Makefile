.PHONY: build run clean test docker-build docker-run docker-stop

# Build the application
build:
	go build -o gomqtt ./cmd/main.go

# Run the application
run:
	go run ./cmd/main.go

# Run the application with debug mode
run-debug:
	go run ./cmd/main.go -config=config/default.json

# Clean up binary
clean:
	rm -f gomqtt

# Run tests
test:
	go test ./...

# Run with coverage report
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

# Initialize development environment
init:
	mkdir -p config web/templates web/static plugins
	cp config/default.json config/config.json

# Docker commands
docker-build:
	docker build -t gomqtt:latest .

docker-run:
	docker-compose up -d

docker-stop:
	docker-compose down

# Generate mock data for testing
mock-data:
	go run ./scripts/mock_data.go 