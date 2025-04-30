#!/bin/bash

# This script runs all the tests for the GoMQTT broker
# Note: Some tests require manual broadcasting of retained messages since the 
# mocked cluster components don't automatically receive the broadcasts from the server.
# This is normal and simulates the real interaction between components.

echo "Running cluster tests..."
go test -v ./cluster

echo "Running MQTT cluster integration tests..."
go test -v ./mqtt -run "Cluster"

echo "Running full integration tests..."
go test -v . -run "TestClusterIntegration"

echo "All tests completed!" 