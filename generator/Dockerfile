# Stage 1: Build the Go binary
FROM golang:1.24-alpine AS builder
WORKDIR /app

# Copy the code and compile a statically linked binary
COPY *.go ./
RUN go mod init generator && go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o telemetry-generator main.go

# Stage 2: Create the minimal production image
FROM alpine:latest
WORKDIR /app

# Pull the compiled binary from the builder stage
COPY --from=builder /app/telemetry-generator .

# Run it
CMD ["./telemetry-generator"]
