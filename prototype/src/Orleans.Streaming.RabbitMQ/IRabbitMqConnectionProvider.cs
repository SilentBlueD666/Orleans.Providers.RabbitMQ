using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ;

public interface IRabbitMqConnectionProvider : IAsyncDisposable
{
    /// <summary>
    /// Gets a new connection.
    /// </summary>
    /// <param name="connectionName">The name of the connection. If null, a default connection name will be used.</param>
    ValueTask<IConnection> GetConnection(string? connectionName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the singleton connection for sending messages.
    /// </summary>
    ValueTask<IConnection> GetSendConnection(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the singleton connection for receiving messages.
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    ValueTask<IConnection> GetReceiveConnection(CancellationToken cancellationToken = default);
}
