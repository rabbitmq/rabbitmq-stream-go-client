package system_integration

import (
	"bytes"
	"fmt"
	"os/exec"
)

type Operations struct {
}

func (o Operations) StopRabbitMQNode(nodeName string) error {
	cmd := exec.Command("vagrant", "ssh", nodeName, "-c", "sudo systemctl stop rabbitmq-server")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return err
	}
	fmt.Printf("res: %q\n", out.String())
	return nil
}

func (o Operations) StartRabbitMQNode(nodeName string) error {
	cmd := exec.Command("vagrant", "ssh", nodeName, "-c", "sudo systemctl start rabbitmq-server")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return err
	}
	fmt.Printf("res: %q\n", out.String())
	return nil
}
