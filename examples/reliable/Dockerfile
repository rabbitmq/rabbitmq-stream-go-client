# Build stage
FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/reliable-client ./examples/reliable

# Runtime stage
FROM gcr.io/distroless/static-debian12

COPY --from=build /out/reliable-client /usr/local/bin/reliable-client

# Connection parameters
ENV RABBITMQ_HOST=localhost
ENV RABBITMQ_PORT=5552
ENV RABBITMQ_USER=guest
ENV RABBITMQ_PASSWORD=guest
ENV RABBITMQ_VHOST=/
ENV RABBITMQ_TLS=false
ENV RABBITMQ_TLS_SKIP_VERIFY=true

# Streams
ENV STREAM_NAMES=golang-reliable-Test,golang-reliable-Test-1,golang-reliable-Test-2
ENV ADD_SUPER_STREAM=true
ENV NUMBER_OF_PARTITIONS=3
ENV SUPER_STREAM_NAME=golang-reliable-super-stream-Test
ENV MAX_STREAM_LENGTH_GB=10

# Load / concurrency
ENV MESSAGES_TO_SEND=1000000
ENV NUMBER_OF_PRODUCERS=2
ENV CONCURRENT_PRODUCERS=1
ENV NUMBER_OF_CONSUMERS=2
ENV SEND_DELAY_MICROS=100
ENV DELAY_EACH_MESSAGES=500
ENV MAX_PRODUCERS_PER_CLIENT=2
ENV MAX_CONSUMERS_PER_CLIENT=5
ENV ENABLE_RESEND=false

# Runtime behaviour
ENV IS_SILENT=true
ENV STATS_INTERVAL_SECONDS=5
ENV PPROF_ENABLED=true
ENV PPROF_ADDR=0.0.0.0:6060

EXPOSE 6060

ENTRYPOINT ["/usr/local/bin/reliable-client"]
