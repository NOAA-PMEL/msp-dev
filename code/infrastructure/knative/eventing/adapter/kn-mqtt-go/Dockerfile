# Stage 1: Build the Go application
FROM golang:bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /kn-mqtt-adapter

# Stage 2: Create the final image
FROM alpine:3.18

WORKDIR /

COPY --from=builder /kn-mqtt-adapter /kn-mqtt-adapter

CMD ["/kn-mqtt-adapter"]