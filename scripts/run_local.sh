#!/usr/bin/env bash
# Runs everything against a RabbitMQ started with docker-compose, without also
# containerizing the producer/consumer -- faster edit-run loop than a full
# `docker compose up --build` while you're working on the .NET code.
set -euo pipefail

docker compose up -d rabbitmq
echo "Waiting for RabbitMQ..."
sleep 5

echo "--- producer ---"
dotnet run --project src/EventPipeline.Producer -- --count=30

echo "--- consumer (Ctrl+C to stop) ---"
dotnet run --project src/EventPipeline.Consumer
