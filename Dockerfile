FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gomqtt ./cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/ /app/
RUN mkdir -p /app/plugins

EXPOSE 1883
EXPOSE 8080
EXPOSE 8081

ENTRYPOINT ["/app/gomqtt"] 