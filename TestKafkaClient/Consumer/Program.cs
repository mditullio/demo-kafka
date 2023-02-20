using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Events;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;

class Consumer
{

    static void Main(string[] args)
    {
        ConsumerConfig configuration = new ConsumerConfig()
        {
            BootstrapServers = "127.0.0.1:9092",
            GroupId = "purchases-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };


        const string topic = Purchase.TOPIC_NAME;

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, Purchase>(configuration)
            .SetValueDeserializer(new JsonDeserializer<Purchase>().AsSyncOverAsync())
            .Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumed event from topic {topic} with key {cr.Message.Key,-10} and value {cr.Message.Value}");
                    }
                    catch (ConsumeException exc)
                    {
                        consumer.Commit(new List<TopicPartitionOffset>()
                        { 
                            exc.ConsumerRecord.TopicPartitionOffset
                        });
                        Console.WriteLine($"Topic partition offset ${exc.ConsumerRecord.TopicPartitionOffset} skipped because of an error");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed.
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
