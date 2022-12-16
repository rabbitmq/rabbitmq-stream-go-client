#!/usr/bin/env bash

optstring=":hi:c:"
container_name="rabbitmq-stream-client-test"

function usage() {
    # shellcheck disable=SC2182
    printf "Usage: start-docker.bash [OPTIONS]\n"
    printf "Utility to start a RabbitMQ container for tests\n"
    printf "Options:\n"
    printf "\t -h \t\t Print this help message\n"
    printf "\t -i string \t Image name to use. Defaults to 'rabbitmq:3.11'\n"
    printf "\t -c string \t Container engine to use. Defaults to 'docker'\n"
}

while getopts ${optstring} arg; do
  case ${arg} in
    h)
      usage
      exit 0
      ;;
    i)
      RABBITMQ_IMAGE="${OPTARG}"
      ;;
    c)
      CONTAINER_RUNTIME="${OPTARG}"
  esac
done

set -ex

"${CONTAINER_RUNTIME:-docker}" run -d \
  -p 127.0.0.1:5552:5552 \
  --name ${container_name} "${RABBITMQ_IMAGE:-rabbitmq:3.11}"

"${CONTAINER_RUNTIME:-docker}" exec --user rabbitmq ${container_name} rabbitmq-plugins enable rabbitmq_stream
