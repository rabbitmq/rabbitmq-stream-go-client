FROM golang:1.8 as builder
ENV GOPATH=/go GOOS=linux CGO_ENABLED=0
WORKDIR /go/src/github.com/rabbitmq/rabbitmq-stream-go-client
COPY go.mod go.sum VERSION ./
COPY pkg pkg
COPY Makefile Makefile
COPY perfTest perfTest

RUN mkdir /stream_perf_test
RUN VERSION=$(cat VERSION) && go build -ldflags "-X main.Version=$VERSION" -o /stream_perf_test/stream-perf-test perfTest/perftest.go

FROM ubuntu:20.04

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		locales



#RUN apt-get install golang -y
#RUN mkdir -p /stream_perf_test
COPY --from=builder /stream_perf_test /bin/
#
RUN 	rm -rf /var/lib/apt/lists/*; \
    	locale-gen en_US.UTF-8


ENTRYPOINT ["stream-perf-test"]
