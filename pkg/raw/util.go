package raw

import (
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"net/url"
	"strconv"
	"strings"
)

func parseURI(uri string) (broker, error) {
	builder := defaultBroker

	if strings.Contains(uri, " ") {
		return builder, errURIWhitespace
	}

	u, err := url.Parse(uri)
	if err != nil {
		return builder, err
	}

	defaultPort, okScheme := schemePorts[u.Scheme]

	if okScheme {
		builder.Scheme = u.Scheme
	} else {
		return builder, errURIScheme
	}

	host := u.Hostname()
	port := u.Port()

	if host != "" {
		builder.Host = host
	}

	if port != "" {
		port32, err := strconv.ParseInt(port, 10, 32)
		if err != nil {
			return builder, err
		}
		builder.Port = int(port32)
	} else {
		builder.Port = defaultPort
	}

	if u.User != nil {
		builder.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			builder.Password = password
		}
	}

	if u.Path != "" {
		if strings.HasPrefix(u.Path, "/") {
			if u.Host == "" && strings.HasPrefix(u.Path, "///") {
				// net/url doesn't handle local context authorities and leaves that up
				// to the scheme handler.  In our case, we translate amqp:/// into the
				// default host and whatever the vhost should be
				if len(u.Path) > 3 {
					builder.Vhost = u.Path[3:]
				}
			} else if len(u.Path) > 1 {
				builder.Vhost = u.Path[1:]
			}
		} else {
			builder.Vhost = u.Path
		}
	}

	// see https://www.rabbitmq.com/uri-query-parameters.html
	params := u.Query()
	builder.AdvHost = params.Get("advHost")
	builder.AdvPort = params.Get("advPort")

	return builder, nil
}

func streamErrorOrNil(responseCode uint16) error {
	err, found := common.ResponseCodeToError[responseCode]
	if !found {
		return fmt.Errorf("unknown response code %d", responseCode)
	}
	return err
}
