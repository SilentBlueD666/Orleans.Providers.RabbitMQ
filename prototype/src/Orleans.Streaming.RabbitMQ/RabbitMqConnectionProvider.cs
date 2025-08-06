using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using RabbitMQ.Client;
using System.Collections.Concurrent;
using System.Threading;

namespace Orleans.Streaming.RabbitMQ.Adapters;

internal sealed partial class RabbitMqConnectionProvider : IRabbitMqConnectionProvider
{
    public const string DefaultConnectionName = "Orleans-Streaming-RabbitMQ";
    public const string DefaultSenderConnectionName = "Orleans-Streaming-RabbitMQ-Sender";
    public const string DefaultReceiverConnectionName = "Orleans-Streaming-RabbitMQ-Receiver";

    private readonly RabbitMqOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RabbitMqConnectionProvider> _logger;

    private readonly ConnectionFactory _factory;
    private readonly List<AmqpTcpEndpoint> _endpoints = [];
    private readonly bool _endpointsConfigured;
    private readonly string _senderConnectionName = DefaultSenderConnectionName;
    private readonly string _receiverConnectionName = DefaultReceiverConnectionName;

    private readonly ConcurrentDictionary<string, ManagedConnection> _connections = new();
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private bool _disposed;

    public RabbitMqConnectionProvider(RabbitMqOptions options, TimeProvider timeProvider, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<RabbitMqConnectionProvider>();

        var clientProvidedName = options.ConnectionNamePrefix;
        if (clientProvidedName is { Length: > 0 })
        {
            _senderConnectionName = $"{clientProvidedName}-Sender";
            _receiverConnectionName = $"{clientProvidedName}-Receiver";
        }

        if (options.UseConnectionString)
        {
            _factory = new ConnectionFactory
            {
                Uri = new Uri(options.ConnectionString!),
                ClientProvidedName = clientProvidedName
            };
        }
        else
        {
            _factory = new ConnectionFactory
            {
                UserName = options.UserName,
                Password = options.Password,
                VirtualHost = options.VirtualHost,
                ClientProvidedName = clientProvidedName
            };

            _endpoints = options
                .Endpoints
                .Select(hostName => AmqpTcpEndpoint.Parse(hostName))
                .ToList();

            _endpointsConfigured = true;
        }

        _options = options;
        _timeProvider = timeProvider;
    }

    private async Task<IConnection> CreateConnection(string connectionName, CancellationToken cancellationToken)
    {
        IConnection connection;
        if (_endpointsConfigured)
        {
            connection = await _factory
                .CreateConnectionAsync(
                    endpoints: _endpoints,
                    clientProvidedName: connectionName,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            connection = await _factory
                .CreateConnectionAsync(
                    clientProvidedName: connectionName,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        LogStartedConnection(connectionName, connection.Endpoint);

        return connection;
    }

    public async ValueTask<IConnection> GetConnection(string? connectionName, CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(RabbitMqConnectionProvider));
        return await CreateConnection(connectionName ?? DefaultConnectionName, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IConnection> GetSendConnection(CancellationToken cancellationToken = default)
        => await GetNamedConnection(_senderConnectionName, cancellationToken).ConfigureAwait(false);

    public async ValueTask<IConnection> GetReceiveConnection(CancellationToken cancellationToken = default)
        => await GetNamedConnection(_receiverConnectionName, cancellationToken).ConfigureAwait(false);

    private async ValueTask<IConnection> GetNamedConnection(string connectionName, CancellationToken cancellationToken)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(RabbitMqConnectionProvider));

        if (_connections.TryGetValue(connectionName, out var managedConnection) && managedConnection.IsHealthy)
        {
            managedConnection.LastUsed = _timeProvider.GetUtcNow();
            return managedConnection.Connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connections.TryGetValue(connectionName, out managedConnection) && managedConnection.IsHealthy)
            {
                managedConnection.LastUsed = _timeProvider.GetUtcNow();
                return managedConnection.Connection;
            }

            try
            {
                if (managedConnection is not null)
                    await managedConnection.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogErrorClosingConnection(ex, connectionName);
            }

            var connection = await CreateConnection(connectionName, cancellationToken).ConfigureAwait(false);

            _connections[connectionName] = new ManagedConnection 
            { 
                Connection = connection,
                LastUsed = _timeProvider.GetUtcNow()
            };

            return connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        await _connectionLock.WaitAsync().ConfigureAwait(false);

        try
        {
            var connectionTasks = _connections
                .Values
                .Select(async managedConnection =>
                {
                    var connectionName = managedConnection.ConnectionName;
                    try
                    {
                        await managedConnection.DisposeAsync().ConfigureAwait(false);
                        LogDisposedConnection(connectionName);
                    }
                    catch (Exception ex)
                    {

                        LogErrorClosingConnection(ex, connectionName);
                    }
                });

            await Task.WhenAll(connectionTasks).ConfigureAwait(false);
            _connections.Clear();

            _disposed = true;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private sealed class ManagedConnection : IAsyncDisposable
    {
        private bool _disposed;

        public required IConnection Connection { get; init; }
        public required DateTimeOffset LastUsed { get; set; }
        public bool IsHealthy => Connection.IsOpen;
        public string ConnectionName => Connection.ClientProvidedName ?? "Unknown";

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            if (Connection.IsOpen)
                await Connection.CloseAsync().ConfigureAwait(false);

            await Connection.DisposeAsync().ConfigureAwait(false);

            _disposed = true;
        }
    }

    [LoggerMessage(
        eventId: 1000,
        level: LogLevel.Debug,
        message: "Started RabbitMQ connection '{ConnectionName}' {Endpoint}")]
    partial void LogStartedConnection(string connectionName, AmqpTcpEndpoint endpoint);

    [LoggerMessage(
        eventId: 1001,
        level: LogLevel.Warning,
        message: "Error while disposing RabbitMQ connection '{ConnectionName}'")]
    partial void LogErrorClosingConnection(Exception ex, string connectionName);

    [LoggerMessage(
        eventId: 1002,
        level: LogLevel.Debug,
        message: "RabbitMQ connection '{ConnectionName}' disposed.")]
    partial void LogDisposedConnection(string connectionName);
}

