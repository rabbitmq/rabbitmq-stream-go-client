package http

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"strconv"
)

type queue struct {
	Messages int `json:"messages"`
}

type connection struct {
	Name string `json:"name"`
}

func messagesReady(queueName string, port string) (int, error) {
	bodyString, err := httpGet("http://localhost:"+port+"/api/queues/%2F/"+queueName, "guest", "guest")
	if err != nil {
		return 0, err
	}

	var data queue
	err = json.Unmarshal([]byte(bodyString), &data)
	if err != nil {
		return 0, err
	}
	return data.Messages, nil
}

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
	return "", errors.New(strconv.Itoa(resp.StatusCode))

}
