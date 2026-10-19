# Infracstructure configuration

Contains everything needed to deploy and run the components

## RedPanda

### How to run
Validate configuration
`docker compose -f infra/compose.yml config`

Start the mock API, Redpanda, Console, Flink, Prometheus, and Grafana
`docker compose -f infra/compose.yml up -d`

On the first run, build the pinned PyFlink image with `docker compose -f infra/compose.yml up -d --build`. Compose creates the topics and submits the Flink processor once it is ready. The Flink Web UI is at http://localhost:8081 and Grafana is at http://localhost:3000. Follow `processor/README.md` for verification and recovery.

Check container status
`docker compose -f infra/compose.yml ps`

Check whether you see the console
http://localhost:8080/overview

Manual topic creation, only when running without the `topics` service:
docker exec eve-redpanda rpk topic create market-observations -p 3 -r 1

Create the alert topic before submitting the V2 processor job:
`docker exec eve-redpanda rpk topic create market.alerts -p 1 -r 1`

Skip either create command if the topic already exists. Inspect alerts with:
`docker exec eve-redpanda rpk topic consume market.alerts -n 1 -o start`

Test message:
'{"item_id":34,"item_name":"Tritanium","current_price":6.97}' |
    docker exec -i eve-redpanda rpk topic produce market-observations


Verify it arrived
docker exec eve-redpanda rpk topic consume market-observations -n 1 -o start


### Context
We start with the quickstart which uses Docker to run. Later on we'll move it to Kubernetes
https://docs.redpanda.com/streaming/current/get-started/quick-start/


https://docs.redpanda.com/labs/docker-compose/single-broker/
Docker setup
https://github.com/redpanda-data/console/blob/master/docs/local/docker-compose.yaml

Kubernetes setup
https://docs.redpanda.com/streaming/current/deploy/redpanda/kubernetes/

Console configuration
https://github.com/redpanda-data/console/blob/master/docs/config/console.yaml



