using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ;

internal sealed class RabbitMqGenericConnector(
    string? name,
    IRabbitMqConnectionProvider connectionProvider,
    ILoggerFactory loggerFactory)
    :
    RabbitMqConnector(loggerFactory),
    IRabbitMqGenericConnector
{
    private readonly string? _name = name;
    private readonly IRabbitMqConnectionProvider _connectionProvider = connectionProvider;
    private readonly ILoggerFactory _loggerFactory = loggerFactory;

    private bool _disposed;
    private IConnection? _connection;
    private SemaphoreSlim _connectionLock = new(1, 1);

    protected override async ValueTask<IConnection> GetConnection(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqGenericConnector));

        if (_connection is { IsOpen: true }) return _connection;

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true }) return _connection;

            if (_connection is not null)
                await _connection.DisposeAsync().ConfigureAwait(false);

            _connection = await _connectionProvider.GetConnection(_name, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _connectionLock.Release();
        }

        return _connection;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await base.DisposeAsync().ConfigureAwait(false);

        await _connectionLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true })
                await _connection.CloseAsync().ConfigureAwait(false);

            if (_connection is not null)
                await _connection.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _connection = null;
            _connectionLock.Release();
        }

        _disposed = true;
    }
}
