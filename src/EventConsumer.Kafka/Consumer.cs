using System;
using Abstractions.EventConsumer;
using Abstractions.EventConsumer.Exceptions;
using Abstractions.Events.Models;
using Confluent.Kafka;
using EventConsumer.Kafka.Configuration;
using Microsoft.Extensions.Options;

namespace EventConsumer.Kafka
{
    public class Consumer : IConsumer
    {
        private ConsumerConfig _consumerConfig;
        private IConsumer<string, Event> _consumerBuilder;
        
        public Consumer(IOptions<KafkaConsumerConfiguration> kafkaConfigOptions)
        {
            if (kafkaConfigOptions == null) 
                throw new ArgumentNullException(
                    nameof(KafkaConsumerConfiguration), 
                    $"{nameof(KafkaConsumerConfiguration)} options object not correctly setup.");
            
            var kafkaConfiguration = kafkaConfigOptions.Value;
            InitializeKafka(kafkaConfiguration);
        }
        
        /// <inheritdoc cref="IConsumer.ConsumptionHandler"/>
        public event Action<Event> ConsumptionHandler;

        /// <inheritdoc cref="IConsumer.Consume"/>
        public void Consume(string topic)
        {
            _consumerBuilder.Subscribe(topic);
            while (true)
            {
                try
                {
                    // set timeout of 0 so it doesnt block the thread, if no result was found, returns null
                    var consumeResult = _consumerBuilder.Consume(0);
                    if (consumeResult == null) continue;
                    
                    var obtainedEvent = consumeResult.Message.Value;
                    ConsumptionHandler?.Invoke(obtainedEvent);
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
            _consumerBuilder = new ConsumerBuilder<string, Event>(_consumerConfig).Build();
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
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                // to be implemented if needed
                _consumerBuilder?.Dispose();
            }
            _disposed = true;
        }
        #endregion
    }
}