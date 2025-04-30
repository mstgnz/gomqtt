#!/bin/bash

echo "Running cluster tests..."
go test -v ./cluster

echo "Running MQTT cluster integration tests..."
go test -v ./mqtt -run "Cluster"

echo "Running full integration tests..."
go test -v . -run "TestClusterIntegration"

echo "All tests completed!" 