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
            var numConsumed = 0;
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumed event from topic {topic}" + Environment.NewLine +
                            $" - key {cr.Message.Key,-10}" + Environment.NewLine +
                            $" - value {cr.Message.Value}" + Environment.NewLine);
                    }
                    catch (ConsumeException exc)
                    {
                        consumer.Commit(new List<TopicPartitionOffset>()
                        { 
                            exc.ConsumerRecord.TopicPartitionOffset
                        });
                        Console.WriteLine($"WARNING: Topic partition offset ${exc.ConsumerRecord.TopicPartitionOffset} skipped because of an error");
                    }

                    consumer.Commit();
                    numConsumed++;

                    if (numConsumed % 5 == 0)
                    {
                        Console.WriteLine($"{numConsumed} messages were consumed from topic {topic}" + Environment.NewLine);
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
