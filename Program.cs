﻿using System;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;
using br.com.foo.kafka.avro;


namespace confluent_kafka_consumer
{
    class Program
    {
        static void Main(string[] args)
        {                  
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "https://schema-registry:8181",
                RequestTimeoutMs = 5000,
                MaxCachedSchemas = 10,
                SslCaLocation = "C:/Users/raul/workspace/kafka-ssl-compose/secrets/CAroot.pem",
                SslKeystoreLocation = "C:/Users/raul/workspace/kafka-ssl-compose/secrets/schema-registry.keystore.jks",
                SslKeystorePassword = "datahub"
            };


            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "kafka-ssl:9092",
                ClientId = "1020",
                SecurityProtocol = SecurityProtocol.Ssl,
                SslCaLocation = "/Users/raul/Developer/workspace-opensource/kafka-ssl-compose/tmp/datahub-ca.crt",
                SslKeystoreLocation = "/Users/raul/Developer/workspace-opensource/kafka-ssl-compose/secrets/consumer.keystore.jks",
                SslKeystorePassword = "datahub",
                Debug = "consumer,cgrp,topic,fetch",
                //Debug = "security",
                GroupId = "clinet-consumer-group",
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                LogConnectionClose = false,
            };


            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))


            using (var consumer = new ConsumerBuilder<Null, Cliente>(consumerConfig)
                    .SetErrorHandler((_, e) =>
                    {
                        Console.WriteLine($"Error Handler");
                        Console.WriteLine($"Error: {e.Reason}");
                    })
                    .SetValueDeserializer(new AvroDeserializer<Cliente>(schemaRegistry).AsSyncOverAsync())
                    .Build())
            {
                consumer.Subscribe("client");
                while (true)
                {
                    try
                    {
                        Console.WriteLine("Consumed has started");
                        var cr = consumer.Consume();
                        Console.WriteLine("Consumed has finished");


                        Console.WriteLine($"ClientId '{cr.Message.Value.clienteId}' and Name '{cr.Message.Value.name}' at: '{cr.TopicPartitionOffset}'.");
                        consumer.Commit(cr);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine("Consume Exception");
                        Console.WriteLine(e.ToString());
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                    catch (KafkaException e)
                    {
                        Console.WriteLine("KafkaException");
                        Console.WriteLine(e.ToString());
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception");
                        Console.WriteLine(e.ToString());
                        Console.WriteLine($"Error occured: {e.Message}");
                    }
                }
            }
        }
    }
}
