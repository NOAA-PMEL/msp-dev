# Build Stage
FROM golang:1.21-alpine AS builder

# Install CA certificates for potential TLS connections
RUN apk add -U --no-cache ca-certificates

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
# Compile a completely static binary for scratch
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o telemetry-proxy main.go

# Final Stage (Scratch)
FROM scratch

# Import CA certs from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Import the compiled static binary
COPY --from=builder /app/telemetry-proxy /telemetry-proxy

# Run the proxy service on container startup
ENTRYPOINT ["/telemetry-proxy"]