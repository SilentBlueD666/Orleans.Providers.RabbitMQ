using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ;

internal interface IRabbitMqConnector : IAsyncDisposable
{
    ValueTask<IChannel> GetChannel(CancellationToken cancellationToken = default);

    ValueTask InitChannel(CancellationToken cancellationToken = default);
}
