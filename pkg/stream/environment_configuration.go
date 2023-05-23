package stream

import (
	"net/url"
	"strconv"
)

// Default values used in EnvironmentConfiguration instead of zero-values
const (
	DefaultUri                              = "rabbitmq-stream://guest:guest@localhost:5552/%2f"
	DefaultHost                             = "localhost"
	DefaultPort                             = 5552
	DefaultUsername                         = "guest"
	DefaultPassword                         = "guest"
	DefaultVirtualHost                      = "/"
	DefaultMaxProducersByConnection         = 255
	DefaultMaxTrackingConsumersByConnection = 50
	DefaultMaxConsumersByConnection         = 255
	DefaultLazyInitialization               = false
	DefaultId                               = "rabbitmq-stream"
)

type EnvironmentConfiguration struct {
	// The URI of the nodes to try to connect to (cluster). This takes precedence
	// over URI and Host + Port.
	//
	// If Uris, Uri, Host and Port are zero-values, it will
	// default to "rabbitmq-stream://guest:guest@localhost:5552/%2f"
	Uris []string
	// The URI of the node to connect to (single node). This takes precedence over Host + Port.
	//
	// If Uris, Uri, Host and Port are zero-values, it will
	// default to "rabbitmq-stream://guest:guest@localhost:5552/%2f"
	Uri string
	// Host to connect to. Uris and Uri take precedence over this. Leave Uris and Uri unset
	// to use this and Port for connection.
	//
	// If Uris, Uri, Host and Port are zero-values, it will
	// default to "localhost"
	Host string
	// Port to use. Uris and Uri take precedence over this. Leave Uris and Uri unset
	// to use this and Host for connection.
	//
	// If Uris, Uri, Host and Port are zero-values, it will
	//default to 5552
	Port int
	// Username to use to connect.
	//
	// Default: "guest"
	Username string
	// Password to use to connect
	//
	// Default: "guest"
	Password string
	// Virtual host to connect to
	//
	// Default: "/"
	VirtualHost string
	// The maximum number of `Producer` instances a single connection can maintain
	// before a new connection is open. The value must be between 1 and 255
	//
	// Default: 255
	MaxProducersByConnection int
	// The maximum number of `Consumer` instances that store their offset a single
	// connection can maintain before a new connection is open. The value must be
	// between 1 and 255
	//
	// Default: 50
	MaxTrackingConsumersByConnection int
	// The maximum number of `Consumer` instances a single connection can maintain
	// before a new connection is open. The value must be between 1 and 255
	//
	// Default: 255
	MaxConsumersByConnection int
	// To delay the connection opening until necessary
	//
	// Default: false
	LazyInitialization bool
	// Informational ID for the environment instance. Used as a prefix for connection
	// names
	//
	// Default: "rabbitmq-stream"
	Id string
}

type EnvironmentConfigurationOption func(*EnvironmentConfiguration)

// WithUri configures the environment with the attributes from the URI. URI must
// conform to the general form:
//
//	[scheme:][//[userinfo@]host][/]path
//
// Whilst it is possible to write a URI as "/some-vhost", it is advisable to
// write the full URI to avoid ambiguities during parsing.
func WithUri(uri string) EnvironmentConfigurationOption {
	return func(c *EnvironmentConfiguration) {
		u, err := url.Parse(uri)
		if err != nil {
			return
		}

		c.Host = u.Hostname()
		if p := u.Port(); len(p) > 0 {
			// url.Parse has taken care of validations
			c.Port, _ = strconv.Atoi(p)
		}

		if user := u.User.Username(); len(user) > 0 {
			c.Username = user
		}

		if pass, isSet := u.User.Password(); isSet {
			c.Password = pass
		}

		if vh := u.Path; len(vh) > 0 {
			// url.Path has a leading slash. The default URI results in Path "//"
			// if the URI omits the path, url.Path will be set to "/"
			// if URI has encoded path as "%2F", url.Path will be set to "//"
			if len(vh) == 1 {
				c.VirtualHost = vh
			} else {
				c.VirtualHost = vh[1:]
			}
		}

		c.Uri = uri
		c.Uris = []string{uri}
	}
}

// WithUris configures the environment with the attributes from the first URI,
// and keeps the other URIs. Having multiple URIs is useful in clusters, so that
// different URIs can be tried if one RabbitMQ server becomes unavailable.
//
// URI must conform to the general form:
//
//	[scheme:][//[userinfo@]host][/]path
//
// Whilst it is possible to write a URI as "/some-vhost", it is advisable to
// write the full URI to avoid ambiguities during parsing.
func WithUris(uris ...string) EnvironmentConfigurationOption {
	return func(c *EnvironmentConfiguration) {
		if len(uris) == 0 {
			return
		}

		for i, uri := range uris {
			uri := uri // TODO(Zerpet): I'm not sure if this is necessary
			if i == 0 {
				WithUri(uri)(c)
				continue
			}
			c.Uris = append(c.Uris, uri)
		}
	}
}

// WithMaxProducersByConnection configures the environment with N maximum number
// of producers per connection. Additional connections will be created when this
// number is reached.
//
// Valid range is 1 <= N <= 255. If N < 1, max producers per connection will be
// set to 1. If N > 255, will be set to 255. Set to N otherwise.
func WithMaxProducersByConnection(n int) EnvironmentConfigurationOption {
	return func(c *EnvironmentConfiguration) {
		if n < 1 {
			c.MaxProducersByConnection = 1
			return
		}
		if n > 255 {
			c.MaxProducersByConnection = 255
			return
		}
		c.MaxProducersByConnection = n
	}
}

// WithMaxTrackingConsumersByConnection configures the environment with N maximum
// number of tracking consumers per connection. Additional connections will be
// created when this number is reached.
//
// Consumers that don't use automatic offset tracking strategy do not count
// towards this limit.
//
// Valid range is 1 <= N <= 255. If N < 1, max tracking consumers per connection
// will be set to 1. If N > 255, will be set to 255. Set to N otherwise.
func WithMaxTrackingConsumersByConnection(n int) EnvironmentConfigurationOption {
	return func(c *EnvironmentConfiguration) {
		if n < 1 {
			c.MaxTrackingConsumersByConnection = 1
			return
		}
		if n > 255 {
			c.MaxTrackingConsumersByConnection = 255
			return
		}
		c.MaxTrackingConsumersByConnection = n
	}
}

// WithMaxConsumersByConnection configures the environment with N maximum number
// of consumers per connection. Additional connections will be created when this
// number is reached.
//
// Valid range is 1 <= N <= 255. If N < 1, max consumers per connection will be
// set to 1. If N > 255, will be set to 255. Set to N otherwise.
func WithMaxConsumersByConnection(n int) EnvironmentConfigurationOption {
	return func(c *EnvironmentConfiguration) {
		if n < 1 {
			c.MaxConsumersByConnection = 1
			return
		}
		if n > 255 {
			c.MaxConsumersByConnection = 255
			return
		}
		c.MaxConsumersByConnection = n
	}
}

// WithLazyInitialization configures the environment to use lazy initialization.
// With lazy initialization enabled, it will delay the connection opening until
// necessary.
func WithLazyInitialization(lazy bool) EnvironmentConfigurationOption {
	return func(c *EnvironmentConfiguration) {
		c.LazyInitialization = lazy
	}
}

// WithId configures the environment informational ID for the environment
// instance. Used as a prefix for connection names.
func WithId(id string) EnvironmentConfigurationOption {
	return func(c *EnvironmentConfiguration) {
		c.Id = id
	}
}

func NewEnvironmentConfiguration(options ...EnvironmentConfigurationOption) *EnvironmentConfiguration {
	e := &EnvironmentConfiguration{
		Uris:                             []string{DefaultUri},
		Uri:                              DefaultUri,
		Host:                             DefaultHost,
		Port:                             DefaultPort,
		Username:                         DefaultUsername,
		Password:                         DefaultPassword,
		VirtualHost:                      DefaultVirtualHost,
		MaxProducersByConnection:         DefaultMaxProducersByConnection,
		MaxTrackingConsumersByConnection: DefaultMaxTrackingConsumersByConnection,
		MaxConsumersByConnection:         DefaultMaxConsumersByConnection,
		LazyInitialization:               DefaultLazyInitialization,
		Id:                               DefaultId,
	}

	for _, option := range options {
		option(e)
	}

	return e
}
