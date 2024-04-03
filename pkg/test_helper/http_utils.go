package test_helper

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"strconv"
)

//type queue struct {
//	Messages int `json:"messages"`
//}

type client_properties struct {
	Connection_name string `json:"connection_name"`
}

type connection struct {
	Name             string            `json:"name"`
	ClientProperties client_properties `json:"client_properties"`
}

//func messagesReady(queueName string, port string) (int, error) {
//	bodyString, err := httpGet("http://localhost:"+port+"/api/queues/%2F/"+queueName, "guest", "guest")
//	if err != nil {
//		return 0, err
//	}
//
//	var data queue
//	err = json.Unmarshal([]byte(bodyString), &data)
//	if err != nil {
//		return 0, err
//	}
//	return data.Messages, nil
//}

func Connections(port string) ([]connection, error) {
	bodyString, err := httpGet("http://localhost:"+port+"/api/connections/", "guest", "guest")
	if err != nil {
		return nil, err
	}

	var data []connection
	err = json.Unmarshal([]byte(bodyString), &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func DropConnectionClientProvidedName(clientProvidedName string, port string) error {
	connections, err := Connections(port)
	if err != nil {
		return err
	}
	connectionToDrop := ""
	for _, connection := range connections {
		if connection.ClientProperties.Connection_name == clientProvidedName {
			connectionToDrop = connection.Name
			break
		}
	}

	if connectionToDrop == "" {
		return errors.New("connection not found")
	}

	err = DropConnection(connectionToDrop, port)
	if err != nil {
		return err
	}

	return nil
}

func DropConnection(name string, port string) error {
	_, err := httpDelete("http://localhost:"+port+"/api/connections/"+name, "guest", "guest")
	if err != nil {
		return err
	}

	return nil
}
func httpGet(url, username, password string) (string, error) {
	return baseCall(url, username, password, "GET")
}

func httpDelete(url, username, password string) (string, error) {
	return baseCall(url, username, password, "DELETE")
}

func baseCall(url, username, password string, method string) (string, error) {
	var client http.Client
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(username, password)

	resp, err3 := client.Do(req)

	if err3 != nil {
		return "", err3
	}

	defer resp.Body.Close()

	if resp.StatusCode == 200 { // OK
		bodyBytes, err2 := ioutil.ReadAll(resp.Body)
		if err2 != nil {
			return "", err2
		}
		return string(bodyBytes), nil
	}

	if resp.StatusCode == 204 { // No Content
		return "", nil
	}

	return "", errors.New(strconv.Itoa(resp.StatusCode))

}
