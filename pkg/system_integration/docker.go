package system_integration

import (
	"fmt"
	"log"
	"os/exec"
	"time"
)

type Operations struct {
}

func getDockerName(port string) string {
	node := getNodeByPort(port)

	return fmt.Sprintf("comopose_rabbit_%s_1", node)

}

func getNodeByPort(port string) string {
	node := "node0"
	switch port {
	case "5553":
		node = "node1"
	case "5554":
		node = "node2"
	}
	return node
}

func (o Operations) StopRabbitMQNode(port string) error {
	cmd := exec.Command("docker", "stop", getDockerName(port))
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("cmd.Run() failed with %s\n", err)
	}
	fmt.Printf("docker: %s\n", string(out))
	time.Sleep(3 * time.Second)
	return nil
}

func (o Operations) StartRabbitMQNode(port string) error {
	cmd := exec.Command("docker", "start", getDockerName(port))
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("cmd.Run() failed with %s\n", err)
	}
	fmt.Printf("docker: %s\n", string(out))
	time.Sleep(2 * time.Second)
	return nil
}

func (o Operations) DeleteReplica(port string, streamName string) error {
	cmd := exec.Command("docker", "exec",
		getDockerName(port), "/bin/bash", "-c",
		"rabbitmq-streams delete_replica "+streamName+" rabbit@"+getNodeByPort(port))
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("cmd.Run() failed with %s\n", err)
	}
	fmt.Printf("docker: %s\n", string(out))
	time.Sleep(2 * time.Second)
	return nil
}

func (o Operations) InstallTools(port string) error {
	cmd := exec.Command("docker", "exec",
		getDockerName(port), "/bin/bash", "-c",
		"apt-get update -y && apt-get install iptables -y")
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("cmd.Run() failed with %s\n", err)
	}
	fmt.Printf("docker: %s\n", string(out))
	time.Sleep(2 * time.Second)
	return nil
}
