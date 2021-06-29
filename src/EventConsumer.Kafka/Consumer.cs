using System;
using Abstractions.EventConsumer;
using Abstractions.EventConsumer.Exceptions;
using Abstractions.Events.Models;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using EventConsumer.Kafka.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace EventConsumer.Kafka
{
    public sealed class Consumer : IConsumer
    {
        private readonly ILogger<Consumer> _logger;

        private ConsumerConfig? _consumerConfig;
        private IConsumer<string, Event>? _consumerBuilder;
        private ISchemaRegistryClient? _schemaRegistry;

        public Consumer(IOptions<KafkaConsumerConfiguration> kafkaConfigOptions, ILogger<Consumer> logger)
        {
            _logger = logger;
            
            var kafkaConfiguration = kafkaConfigOptions.Value;
            InitializeKafka(kafkaConfiguration);
        }

        /// <inheritdoc cref="IConsumer.ConsumptionHandler"/>
        public event Action<Event>? ConsumptionHandler;

        /// <inheritdoc cref="IConsumer.Consume"/>
        public void Consume(string topic)
        {
            _consumerBuilder?.Subscribe(topic);
            while (true)
            {
                try
                {
                    // set timeout of 0 so it doesnt block the thread, if no result was found, returns null
                    var consumeResult = _consumerBuilder?.Consume(0);
                    if (consumeResult == null) continue;

                    ConsumptionHandler?.Invoke(consumeResult.Message.Value);
                }
                catch (ConsumeException e)
                {
                    // only throw exception is the error is fatal (aka it's in an unrecoverable state)
                    if (e.Error.IsFatal) throw new ConsumerException(e.Message, e);
                }
            }
        }

        #region PRIVATE METHODS
        private void InitializeKafka(KafkaConsumerConfiguration kafkaConfiguration)
        {
            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = ThrowIfEmpty(kafkaConfiguration.BootstrapServerUrls,
                                                $"{nameof(KafkaConsumerConfiguration)} bootstrap server urls cannot be null or empty."),
                GroupId = ThrowIfEmpty(kafkaConfiguration.GroupId,
                                       $"{nameof(KafkaConsumerConfiguration)} group id cannot be null or empty."),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            _schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = ThrowIfEmpty(kafkaConfiguration.AvroSchemaRegistryUrls,
                                   $"{nameof(KafkaConsumerConfiguration)} schema registry urls cannot be null or empty.")
            });
            _consumerBuilder = new ConsumerBuilder<string, Event>(_consumerConfig)
               .SetErrorHandler(HandleErrors())
               .SetKeyDeserializer(new AvroDeserializer<string>(_schemaRegistry).AsSyncOverAsync())
               .SetValueDeserializer(new AvroDeserializer<Event>(_schemaRegistry).AsSyncOverAsync())
               .Build();
        }

        private Action<IConsumer<string, Event>, Error> HandleErrors()
        {
            return (_, error) => _logger.LogError("Kafka encountered an error: {@Error}", error);
        }

        private static string ThrowIfEmpty(string value, string errorMessage)
        {
            if (string.IsNullOrEmpty(value)) throw new ArgumentException(errorMessage);
            return value;
        }
        #endregion

        #region DISPOSE PATTERN
        private bool _disposed;

        /**
         * Consumer dispose method
         */
        public void Dispose()
        {
            Dispose(true);
        }

        /**
         * How we dispose an object
         */
        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                // to be implemented if needed
                _consumerBuilder?.Dispose();
                _schemaRegistry?.Dispose();
            }

            _disposed = true;
        }
        #endregion
    }
}