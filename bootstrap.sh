#!/usr/bin/env sh
set -e

TEST_HOST="${TEST_HOST:-127.0.0.1}"

get_exposed_port() {
    docker-compose port "$@" | cut -d: -f2
}

rm -rf build && mkdir build

docker-compose down --timeout 0 --volumes --remove-orphans
docker-compose pull -q
docker-compose up -d

echo "Environment variables (build/test-environment):"
tee build/test-environment << EOF
export AMQP_EXCHANGE=amq.topic
export AMQP_URL=amqp://guest:guest@$TEST_HOST:$(get_exposed_port rabbitmq 5672)/%2f
export ASYNC_TEST_TIMEOUT=10
export RABBIMQ_URL=http://guest:guest@$TEST_HOST:$(get_exposed_port rabbitmq 15672)
EOF

echo 'Bootstrap complete'
