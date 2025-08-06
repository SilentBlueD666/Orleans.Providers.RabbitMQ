using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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
    private readonly ILoggerFactory _loggerFactory;
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
        _loggerFactory = loggerFactory;
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

            _connections[connectionName] = ManagedConnection.Create(
                connection: connection,
                timeProvider: _timeProvider,
                loggerFactory: _loggerFactory);

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

    private sealed partial class ManagedConnection : IAsyncDisposable
    {
        private readonly ILogger<ManagedConnection> _logger;
        private bool _disposed;

        public ManagedConnection(IConnection connection, ILogger<ManagedConnection> logger)
        {
            _logger = logger;
            Connection = connection;
            Connection.ConnectionShutdownAsync += OnConnectionShutdown;
            Connection.ConnectionBlockedAsync += OnConnectionBlocked;
            Connection.ConnectionUnblockedAsync += OnConnectionUnblocked;
        }

        public IConnection Connection { get; }
        public DateTimeOffset LastUsed { get; set; }
        public bool IsHealthy => !_disposed && Connection.IsOpen;
        public string ConnectionName => Connection.ClientProvidedName ?? "Unknown";

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            if (Connection.IsOpen)
                await Connection.CloseAsync().ConfigureAwait(false);

            Connection.ConnectionShutdownAsync -= OnConnectionShutdown;
            Connection.ConnectionBlockedAsync -= OnConnectionBlocked;
            Connection.ConnectionUnblockedAsync -= OnConnectionUnblocked;

            await Connection.DisposeAsync().ConfigureAwait(false);

            _disposed = true;
        }

        public static ManagedConnection Create(IConnection connection, TimeProvider timeProvider, ILoggerFactory loggerFactory)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(timeProvider);
            ArgumentNullException.ThrowIfNull(loggerFactory);

            return new ManagedConnection(connection: connection, logger: loggerFactory.CreateLogger<ManagedConnection>())
            {
                LastUsed = timeProvider.GetUtcNow()
            };
        }

        private Task OnConnectionShutdown(object sender, ShutdownEventArgs @event)
        {
            var reason = @event.ToString();

            var exception = @event.Exception;
            if (exception is null)
                LogConnectionShutdown(ConnectionName, reason);
            else
                LogConnectionShutdownError(ConnectionName, reason, exception);

            return Task.CompletedTask;
        }

        private Task OnConnectionBlocked(object sender, ConnectionBlockedEventArgs @event)
        {
            LogConnectionBlocked(ConnectionName, @event.Reason);
            return Task.CompletedTask;
        }

        private Task OnConnectionUnblocked(object sender, AsyncEventArgs @event)
        {
            LogConnectionUnblocked(ConnectionName);
            return Task.CompletedTask;
        }

        [LoggerMessage(
            eventId: 1000,
            level: LogLevel.Information,
            message: "RabbitMQ connection '{ConnectionName}' shutdown: {Reason}")]
        partial void LogConnectionShutdown(string connectionName, string reason);

        [LoggerMessage(
            eventId: 1001,
            level: LogLevel.Error,
            message: "RabbitMQ connection '{ConnectionName}' shutdown with error: {Reason}")]
        partial void LogConnectionShutdownError(string connectionName, string reason, Exception ex);

        [LoggerMessage(
            eventId: 1002,
            level: LogLevel.Warning,
            message: "RabbitMQ connection '{ConnectionName}' blocked: {Reason}")]
        partial void LogConnectionBlocked(string connectionName, string reason);

        [LoggerMessage(
            eventId: 1003,
            level: LogLevel.Information,
            message: "RabbitMQ connection '{ConnectionName}' unblocked.")]
        partial void LogConnectionUnblocked(string connectionName);
    }

    [LoggerMessage(
        eventId: 1000,
        level: LogLevel.Information,
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

