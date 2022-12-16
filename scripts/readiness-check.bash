#!/usr/bin/env bash

optstring=":hc:"
container_name="rabbitmq-stream-client-test"

function usage() {
    # shellcheck disable=SC2182
    printf "Usage: readiness-check.bash [OPTIONS]\n"
    printf "Utility to check readiness of a RabbitMQ container\n"
    printf "Options:\n"
    printf "\t -h \t\t Print this help message\n"
    printf "\t -c string \t Container engine to use. Defaults to 'docker'\n"
}

while getopts ${optstring} arg; do
  case ${arg} in
    h)
      usage
      exit 0
      ;;
    c)
      CONTAINER_RUNTIME="${OPTARG}"
  esac
done

set -ex

"${CONTAINER_RUNTIME:-docker}" exec ${container_name} rabbitmq-diagnostics check_port_listener 5552
