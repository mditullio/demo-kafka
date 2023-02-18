using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Events;
using System.Net;

var config = new AdminClientConfig()
{
    BootstrapServers = "127.0.0.1:9092",
};

using var client = new AdminClientBuilder(config).Build();

var topicSpec = new TopicSpecification
{
    Name = Purchase.TOPIC_NAME,
    NumPartitions = 2,
    ReplicationFactor = 1,
};

await client.CreateTopicsAsync(new List<TopicSpecification>() { topicSpec });

Console.WriteLine($"Topic '{topicSpec.Name}' created succesfully.");
