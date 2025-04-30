.PHONY: build install clean

# Set the binary name
BINARY_NAME=gomqtt-cli

# Set module path
MODULE_PATH=github.com/mstgnz/gomqtt

# Set directory for binary
BINDIR=${GOPATH}/bin

# Build the CLI tool
build:
	@echo "Building ${BINARY_NAME}..."
	go build -o ${BINARY_NAME} .

# Install the CLI tool
install: build
	@echo "Installing ${BINARY_NAME}..."
	cp ${BINARY_NAME} ${BINDIR}/${BINARY_NAME}

# Update dependencies
deps:
	@echo "Updating dependencies..."
	go get -u github.com/spf13/cobra

# Run the CLI tool
run: build
	@echo "Running ${BINARY_NAME}..."
	./${BINARY_NAME}

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f ${BINARY_NAME}
	
# Help text
help:
	@echo "GoMQTT CLI Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  build    - Build the CLI binary"
	@echo "  install  - Install the CLI binary to GOPATH/bin"
	@echo "  deps     - Update dependencies"
	@echo "  run      - Build and run the CLI"
	@echo "  clean    - Remove build artifacts"

# Default target
default: build 