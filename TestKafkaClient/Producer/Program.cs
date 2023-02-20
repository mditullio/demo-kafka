
using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using Events;
using Newtonsoft.Json;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Confluent.Kafka.SyncOverAsync;
using NJsonSchema.Generation;

class Producer
{
    static void Main(string[] args)
    {
        ProducerConfig producerConfig = new ProducerConfig()
        {
            BootstrapServers = "127.0.0.1:9092",
        };

        JsonSerializerConfig serializerConfig = new JsonSerializerConfig()
        {
            AutoRegisterSchemas = true,            
        };

        JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = new JsonSchemaGeneratorSettings()
        {
        };

        const string topic = Purchase.TOPIC_NAME;

        string[] users = { 
            "Marco", 
            "Daniel", 
            "Jérome", 
            "Cédric",
            "Mamadou",
            "Olivier", 
            "Pascal" 
        };

        Item[] items = new Item[] {
            new Item("Book", 20.0),
            new Item("Alarm clock", 15.0),
            new Item("T-shirt", 15.0),
            new Item("Gift card", 100.0),
            new Item("Batteries AA", 5.0)
        };
        
        using (var schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig() { Url = "127.0.0.1:8081" }))
        using (var producer = new ProducerBuilder<string, Purchase>(producerConfig)
            .SetValueSerializer(new JsonSerializer<Purchase>(schemaRegistryClient, serializerConfig, jsonSchemaGeneratorSettings).AsSyncOverAsync())
            .Build())
        {
            var numProduced = 0;
            Random rnd = new Random();
            const int numMessages = 10;
            for (int i = 0; i < numMessages; ++i)
            {
                var purchase = new Purchase
                {
                    UserId = users[rnd.Next(users.Length)]
                };

                for (int j = 0; j < 3; j++)
                {
                    purchase.Items.Add(items[rnd.Next(items.Length)]);
                }

                purchase.Amount = purchase.Items.Sum(item => item.Price);

                producer.Produce(topic, new Message<string, Purchase> { Key = purchase.UserId, Value = purchase },
                    (deliveryReport) =>
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                        {
                            Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                        }
                        else
                        {
                            Console.WriteLine($"Produced event to topic {topic}:" + Environment.NewLine +
                                $" - key = {purchase.UserId,-10}" + Environment.NewLine +
                                $" - value = {purchase}" + Environment.NewLine);

                            numProduced += 1;
                        }
                    });

                producer.Flush();
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}" + Environment.NewLine);
        }
    }
}