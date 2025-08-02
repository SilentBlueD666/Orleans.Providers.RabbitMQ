using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ;

internal sealed class RabbitMqConsumerConnector(
    IRabbitMqConnectionProvider connectionProvider,
    RabbitMqOptions options,
    ILoggerFactory loggerFactory)
    :
    RabbitMqConnector(loggerFactory), 
    IRabbitMqConsumerConnector
{
    private readonly IRabbitMqConnectionProvider _connectionProvider = connectionProvider;
    private readonly RabbitMqOptions _options = options;

    protected override async ValueTask<IConnection> GetConnection(CancellationToken cancellationToken = default)
        => await _connectionProvider
            .GetReceiveConnection(cancellationToken)
            .ConfigureAwait(false);

    protected override async ValueTask OnChannelCreated(IChannel channel, CancellationToken cancellationToken = default)
    {
        if (_options.PrefetchCount > 0)
            await channel.BasicQosAsync(0, (ushort)_options.PrefetchCount, false, cancellationToken).ConfigureAwait(false);
    }
}