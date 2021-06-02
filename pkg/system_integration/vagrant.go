package system_integration

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"
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
	time.Sleep(5 * time.Second)
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
	time.Sleep(5 * time.Second)
	return nil
}

func (o Operations) DeleteReplica(nodeName string, streamName string) error {
	cmd := exec.Command("vagrant", "ssh", nodeName, "-c", "sudo rabbitmq-streams delete_replica "+
		streamName+"   rabbit@"+nodeName)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return err
	}
	fmt.Printf("res: %q\n", out.String())
	time.Sleep(5 * time.Second)
	return nil
}
