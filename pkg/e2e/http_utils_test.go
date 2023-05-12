package e2e_test

import (
	"fmt"
	hole "github.com/michaelklishin/rabbit-hole/v2"
)

type HTTPUtils struct {
	HttpClient *hole.Client
}

func NewHTTPUtils() *HTTPUtils {
	client, err := hole.NewClient("http://localhost:15672", "guest", "guest")
	if err != nil {
		return nil
	}
	return &HTTPUtils{
		HttpClient: client,
	}
}

func (h *HTTPUtils) getConnectionInfoByName(connectionName string) (*hole.ConnectionInfo, error) {
	connections, err := h.HttpClient.ListConnections()
	if err != nil {
		return nil, err
	}
	for i := range connections {
		if connections[i].ClientProperties["connection_name"] != nil &&
			connections[i].ClientProperties["connection_name"] == connectionName {
			return &connections[i], nil
		}
	}
	return nil, fmt.Errorf("connection not found %s", connectionName)
}

func (h *HTTPUtils) GetConnectionByConnectionName(connectionName string) (string, error) {
	connection, err := h.getConnectionInfoByName(connectionName)
	if err != nil {
		return "", err
	}
	return connection.ClientProperties["connection_name"].(string), nil
}

func (h *HTTPUtils) ForceCloseConnectionByConnectionName(connectionName string) error {

	connection, err := h.getConnectionInfoByName(connectionName)
	if err != nil {
		return err
	}
	_, err = h.HttpClient.CloseConnection(connection.Name)
	return err
}
