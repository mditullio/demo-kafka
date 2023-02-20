
using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using Events;
using Newtonsoft.Json;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Confluent.Kafka.SyncOverAsync;

class Producer
{
    static void Main(string[] args)
    {
        ProducerConfig configuration = new ProducerConfig()
        {
            BootstrapServers = "127.0.0.1:9092",
        };

        const string topic = Purchase.TOPIC_NAME;

        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
        string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };
        double[] amounts = { 20.0, 15.0, 15.0, 100.0, 5.0 };

        using (var schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig() { Url = "127.0.0.1:8081" }))
        using (var producer = new ProducerBuilder<string, Purchase>(configuration)
            .SetValueSerializer(new JsonSerializer<Purchase>(schemaRegistryClient).AsSyncOverAsync())
            .Build())
        {
            var numProduced = 0;
            Random rnd = new Random();
            const int numMessages = 10;
            for (int i = 0; i < numMessages; ++i)
            {
                var purchase = new Purchase();
                purchase.UserId = users[rnd.Next(users.Length)];

                for (int j = 0; j < 3; j++)
                {
                    var next = rnd.Next(items.Length);
                    purchase.Items.Add(items[next]);
                    purchase.Amount += amounts[next];
                }

                producer.Produce(topic, new Message<string, Purchase> { Key = purchase.UserId, Value = purchase },
                    (deliveryReport) =>
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                        {
                            Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                        }
                        else
                        {
                            Console.WriteLine($"Produced event to topic {topic}: key = {purchase.UserId,-10} value = {purchase}");
                            numProduced += 1;
                        }
                    });
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        }
    }
}