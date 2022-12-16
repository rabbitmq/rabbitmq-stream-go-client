#!/usr/bin/env bash

optstring=":hc:p"
container_name="rabbitmq-stream-client-test"

function usage() {
    # shellcheck disable=SC2182
    printf "Usage: stop-docker.bash [OPTIONS]\n"
    printf "Utility to start a RabbitMQ container for tests\n"
    printf "Options:\n"
    printf "\t -h \t\t Print this help message\n"
    printf "\t -c string \t Container engine to use. Defaults to 'docker'\n"
    printf "\t -p \t\t Do not delete the container after stopping\n"
}

while getopts ${optstring} arg; do
  case ${arg} in
    h)
      usage
      exit 0
      ;;
    c)
      CONTAINER_RUNTIME="${OPTARG}"
      ;;
    p)
      PRESERVE_CONTAINER=1
      ;;
  esac
done

set -ex

"${CONTAINER_RUNTIME:-docker}" stop "${container_name}"

if [ "${PRESERVE_CONTAINER}" ]; then
    exit $?
fi

"${CONTAINER_RUNTIME:-docker}" rm "${container_name}"
