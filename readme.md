# Start Kafka broker

Run docker-compose to build containers :

```
docker-compose up -d
```

# Create a topic

```
docker exec broker \
kafka-topics --bootstrap-server broker:9092 \
             --create \
             --topic quickstart
```
# Write messages to the topic

```
docker exec --interactive --tty broker \
kafka-console-producer --bootstrap-server broker:9092 \
                       --topic quickstart \
                       --property parse.key=true \
                       --property key.separator=":"
```

When you’ve finished, press Ctrl-D to return to your command prompt.

# Read messages from the topic

```
docker exec --interactive --tty broker \
kafka-console-consumer --bootstrap-server broker:9092 \
                       --topic quickstart \
                       --from-beginning \
                       --property print.key=true \
                       --property key.separator="-"
```

When you’ve finished, press Ctrl-C to return to your command prompt.

