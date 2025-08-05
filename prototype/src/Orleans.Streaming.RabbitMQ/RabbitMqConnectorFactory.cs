using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using System.Xml.Linq;

namespace Orleans.Streaming.RabbitMQ;

internal sealed class RabbitMqConnectorFactory(
    IRabbitMqConnectionProvider connectionProvider,
    RabbitMqOptions options,
    ILoggerFactory loggerFactory)
    : 
    IRabbitMqConnectorFactory
{
    public IRabbitMqConnector CreateProducerConnector()
        => new RabbitMqConnector(
            connectionProvider: connectionProvider,
            connectorType: RabbitMqConnectorType.Producer,
            options: options,
            loggerFactory: loggerFactory);

    public IRabbitMqConnector CreateConsumerConnector()
        => new RabbitMqConnector(
            connectionProvider: connectionProvider,
            connectorType: RabbitMqConnectorType.Consumer,
            options: options,
            loggerFactory: loggerFactory);

    public IRabbitMqConnector CreateConnector(string? name)
        => new RabbitMqConnector(
            connectionProvider: connectionProvider,
            connectorType: RabbitMqConnectorType.Generic,
            options: options,
            loggerFactory: loggerFactory,
            customConnectionName: name);
}