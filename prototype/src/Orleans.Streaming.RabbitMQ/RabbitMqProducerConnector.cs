using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ;

internal sealed class RabbitMqProducerConnector(
    IRabbitMqConnectionProvider connectionProvider,
    ILoggerFactory loggerFactory)
    :
    RabbitMqConnector(loggerFactory), 
    IRabbitMqProducerConnector
{
    private readonly IRabbitMqConnectionProvider _connectionProvider = connectionProvider;

    protected override async ValueTask<IConnection> GetConnection(CancellationToken cancellationToken = default)
        => await _connectionProvider
            .GetSendConnection(cancellationToken)
            .ConfigureAwait(false);
}