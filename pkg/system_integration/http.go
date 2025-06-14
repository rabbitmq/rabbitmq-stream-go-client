package system_integration

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
)

type queue struct {
	Messages int `json:"messages"`
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

func httpGet(url, username, password string) (string, error) {
	var client http.Client
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(username, password)

	resp, err3 := client.Do(req)

	if err3 != nil {
		return "", err3
	}

	//nolint:errcheck
	defer resp.Body.Close()

	if resp.StatusCode == 200 { // OK
		bodyBytes, err2 := io.ReadAll(resp.Body)
		if err2 != nil {
			return "", err2
		}
		return string(bodyBytes), nil

	}
	return "", errors.New(strconv.Itoa(resp.StatusCode))

}
