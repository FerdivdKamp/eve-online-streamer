# Infracstructure configuration

Contains everything needed to deploy and run the components

## RedPanda

### How to run
Validate configuration
`docker compose -f infra/compose.yml config`

Start Redpanda and Console
`docker compose -f infra/compose.yml up -d`

Check container status
`docker compose -f infra/compose.yml ps`

Check whether you see the console
http://localhost:8080/overview

Creating a test topic
docker exec eve-redpanda rpk topic create market-observations -p 3 -r 1

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



