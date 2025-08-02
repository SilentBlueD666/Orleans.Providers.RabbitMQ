using Microsoft.Extensions.Logging;
using Orleans.Configuration;

namespace Orleans.Streaming.RabbitMQ;

internal sealed class RabbitMqConnectorFactory(
    IRabbitMqConnectionProvider connectionProvider,
    RabbitMqOptions options,
    ILoggerFactory loggerFactory)
    : 
    IRabbitMqConnectorFactory
{
    public IRabbitMqProducerConnector CreateProducerConnector()
        => new RabbitMqProducerConnector(connectionProvider, loggerFactory);

    public IRabbitMqConsumerConnector CreateConsumerConnector()
        => new RabbitMqConsumerConnector(connectionProvider, options, loggerFactory);

    public IRabbitMqConnector CreateConnector(string? name)
        => new RabbitMqGenericConnector(name, connectionProvider, loggerFactory);
}