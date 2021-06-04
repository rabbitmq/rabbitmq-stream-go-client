package system_integration

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"
)

type Operations struct {
}

func getDockerName(node string) string {
	return fmt.Sprintf("comopose_rabbit_%s_1", node)

}

func (o Operations) StopRabbitMQNode(nodeName string) error {
	cmd := exec.Command("docker", "stop", getDockerName(nodeName))
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
	cmd := exec.Command("docker", "start", getDockerName(nodeName))
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return err
	}
	fmt.Printf("res: %q\n", out.String())
	time.Sleep(3 * time.Second)
	return nil
}

func (o Operations) DeleteReplica(nodeName string, streamName string) error {
	cmd := exec.Command("docker", "exec", "-it", getDockerName(nodeName), "/bin/bash", "rabbitmq-streams delete_replica "+
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
