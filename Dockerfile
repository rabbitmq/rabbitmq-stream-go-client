FROM golang:1.16
ENV GOPATH=/go GOOS=linux CGO_ENABLED=0
WORKDIR /go/src/github.com/Gsantomaggio/go-stream-client
COPY go.mod go.sum VERSION ./
COPY pkg pkg
COPY perfTest perfTest

RUN VERSION=$(cat VERSION) && go build -ldflags "-X main.Version=$VERSION" -o /bin/perTest perfTest/perftest.go
RUN echo '#!/bin/bash'>  /bin/pperTest
RUN echo 'perTest "$@"' >>  /bin/pperTest
RUN chmod +x /bin/pperTest
ENTRYPOINT ["pperTest"]

