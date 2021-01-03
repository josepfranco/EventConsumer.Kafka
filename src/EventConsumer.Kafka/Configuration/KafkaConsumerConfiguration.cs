namespace EventConsumer.Kafka.Configuration
{
    public class KafkaConsumerConfiguration
    {
        /// <summary>
        /// The bootstrap server urls in Kafka's format
        /// </summary>
        public string BootstrapServerUrls { get; set; } = string.Empty;

        /// <summary>
        /// The Kafka consumer group 
        /// </summary>
        public string GroupId { get; set; } = string.Empty;
    }
}