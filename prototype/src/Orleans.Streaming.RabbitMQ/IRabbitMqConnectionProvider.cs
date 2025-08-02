using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ;

public interface IRabbitMqConnectionProvider : IAsyncDisposable
{
    /// <summary>
    /// Gets a new connection.
    /// </summary>
    ValueTask<IConnection> GetConnection(string? clientName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the singleton connection for sending (async).
    /// </summary>
    ValueTask<IConnection> GetSendConnection(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a new connection for receiving messages (async).
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    ValueTask<IConnection> GetReceiveConnection(CancellationToken cancellationToken = default);
}
