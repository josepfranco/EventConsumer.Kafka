using Abstractions.EventConsumer;
using EventConsumer.Kafka.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace EventConsumer.Kafka.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaConsumer(this IServiceCollection services,
                                                          IConfiguration configuration)
        {
            services.Configure<KafkaConsumerConfiguration>(configuration.GetSection(nameof(KafkaConsumerConfiguration)));
            services.AddTransient<IConsumer, Consumer>();
            return services;
        }
    }
}