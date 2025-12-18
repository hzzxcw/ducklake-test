#!/bin/bash
echo "Waiting for Kafka Connect to start..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:8083)" != "200" ]]; do sleep 5; done
echo "Kafka Connect is up!"

echo "Registering Debezium MySQL Connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "topic.prefix": "dbserver1",
    "database.include.list": "inventory",
    "schema.history.internal.kafka.bootstrap.servers": "redpanda:29092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory"
  }
}'
echo -e "\nConnector registered."
