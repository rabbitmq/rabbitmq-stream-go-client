//+build debug

package streaming

import "sort"

type ProducersCoordinator = producersCoordinator

func (env *Environment) ClientCoordinator() map[string]*ProducersCoordinator {
	return env.producers.clientCoordinator
}

func (env *Environment) Nodes() []string {
	var result []string
	for s, _ := range env.producers.clientCoordinator {
		result = append(result, s)
	}
	sort.Strings(result)
	return result
}

func (env *Environment) ProducerPerStream(streamName string) []*Producer {
	var result []*Producer
	for _, p := range env.producers.clientCoordinator {
		for _, client := range p.getClientsPerContext() {
			for _, prod := range client.coordinator.producers {
				if prod.(*Producer).parameters.streamName == streamName {
					result = append(result, prod.(*Producer))
				}
			}
		}
	}
	return result
}

func (env *Environment) ClientsPerStream(streamName string) []*Client {
	var result []*Client
	for _, p := range env.producers.clientCoordinator {
		for _, client := range p.getClientsPerContext() {
			for _, prod := range client.coordinator.producers {
				if prod.(*Producer).parameters.streamName == streamName {
					result = append(result, client)
				}
			}
		}
	}
	return result
}
