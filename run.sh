#!/usr/bin/env bash

docker-compose up -d

# Add schema to Avro registry

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": '"$(cat schema/user.avsc | jq -R -s '.')"'}' \
  http://localhost:8081/subjects/user-topic-value/versions

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": '"$(cat schema/contract.avsc | jq -R -s '.')"'}' \
  http://localhost:8081/subjects/contract-topic-value/versions
