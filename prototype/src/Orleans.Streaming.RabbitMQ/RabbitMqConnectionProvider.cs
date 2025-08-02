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

    private readonly SemaphoreSlim _senderLock = new(1, 1);
    private readonly SemaphoreSlim _receiveLock = new(1, 1);
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqConnectionProvider> _logger;

    private readonly ConnectionFactory _factory;
    private readonly List<AmqpTcpEndpoint> _endpoints = [];
    private readonly bool _endpointsConfigured;
    private readonly string _senderConnectionName = DefaultSenderConnectionName;
    private readonly string _receiverConnectionName = DefaultReceiverConnectionName;

    private IConnection? _sendConnection;
    private IConnection? _receiveConnection;

    private bool _disposed;

    public RabbitMqConnectionProvider(RabbitMqOptions options, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<RabbitMqConnectionProvider>();

        var clientProvidedName = options.ConnectionName;
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
                Port = options.Port,
                UserName = options.UserName,
                Password = options.Password,
                VirtualHost = options.VirtualHost,
                ClientProvidedName = clientProvidedName
            };

            _endpoints = options
                .HostNames
                .Select(hostName => new AmqpTcpEndpoint(hostName))
                .ToList();

            _endpointsConfigured = true;
        }

        _options = options;
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

    public async ValueTask<IConnection> GetConnection(string? clientName, CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(RabbitMqConnectionProvider));

        return await CreateConnection(clientName ?? DefaultConnectionName, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IConnection> GetSendConnection(CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(RabbitMqConnectionProvider));

        if (_sendConnection is { IsOpen: true }) return _sendConnection;

        await _senderLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_sendConnection is { IsOpen: true }) return _sendConnection;

            if (_sendConnection is not null)
                await _sendConnection.DisposeAsync().ConfigureAwait(false);

            _sendConnection = await CreateConnection(_senderConnectionName, cancellationToken).ConfigureAwait(false);

            return _sendConnection;
        }
        finally
        {
            _senderLock.Release();
        }
    }

    public async ValueTask<IConnection> GetReceiveConnection(CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(RabbitMqConnectionProvider));

        if (_receiveConnection is { IsOpen: true }) return _receiveConnection;

        await _receiveLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_receiveConnection is { IsOpen: true }) return _receiveConnection;

            if (_receiveConnection is not null)
                await _receiveConnection.DisposeAsync().ConfigureAwait(false);

            _receiveConnection = await CreateConnection(_receiverConnectionName, cancellationToken).ConfigureAwait(false);

            return _receiveConnection;
        }
        finally
        {
            _receiveLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        await _senderLock.WaitAsync().ConfigureAwait(false);
        await _receiveLock.WaitAsync().ConfigureAwait(false);

        try
        {
            if (_disposed) return;

            if (_sendConnection is not null)
            {
                try
                {
                    if (_sendConnection.IsOpen)
                        await _sendConnection.CloseAsync().ConfigureAwait(false);

                    await _sendConnection.DisposeAsync().ConfigureAwait(false);
                }
                catch(Exception ex)
                {
                    LogErrorClosingConnection(ex, _senderConnectionName);
                }

                _sendConnection = null;
            }

            if (_receiveConnection is not null)
            {
                try
                {
                    if (_receiveConnection.IsOpen)
                        await _receiveConnection.CloseAsync().ConfigureAwait(false);

                    await _receiveConnection.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogErrorClosingConnection(ex, _receiverConnectionName);
                }

                _receiveConnection = null;
            }
        }
        finally
        {
            _senderLock.Release();
            _receiveLock.Release();
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
}