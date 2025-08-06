using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Data.Common;

namespace Orleans.Streaming.RabbitMQ;

internal sealed partial class RabbitMqConnector(
    IRabbitMqConnectionProvider connectionProvider,
    RabbitMqConnectorType connectorType,
    RabbitMqOptions options,
    ILoggerFactory loggerFactory,
    string? customConnectionName = null)
    :
    IRabbitMqConnector
{
    public const string DefaultConnectorName = "Orleans-Streaming-RabbitMQ-Connector";

    private readonly ILogger<RabbitMqConnector> _logger = loggerFactory.CreateLogger<RabbitMqConnector>();
    private readonly IRabbitMqConnectionProvider _connectionProvider = connectionProvider;
    private readonly RabbitMqConnectorType _connectorType = connectorType;
    private readonly RabbitMqOptions _options = options;
    private readonly string? _customConnectionName = customConnectionName;

    private ManagedChannel? _channel;
    private SemaphoreSlim _channelLock = new(1, 1);
    private string _connectionName = string.Empty;
    private IConnection? _connection;

    private bool _disposed;

    public async ValueTask InitChannel(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqConnector));

        await _channelLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_channel is { IsHealthy: true })
                return;

            if (_channel is not null)
                await _channel.DisposeAsync().ConfigureAwait(false);

            await InitConnection(cancellationToken).ConfigureAwait(false);
            if (_connection is null || !_connection.IsOpen)
                throw new InvalidOperationException("Connection is not initialized or not open.");

            var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            if (_connectorType == RabbitMqConnectorType.Consumer && _options.PrefetchCount > 0)
                await channel.BasicQosAsync(0, (ushort)_options.PrefetchCount, false, cancellationToken).ConfigureAwait(false);

            _channel = ManagedChannel.Create(
                channel: channel,
                connectionName: _connectionName,
                loggerFactory: loggerFactory);

            LogChannelCreated(_channel.ChannelNumber, _connectionName, _connectorType);
        }
        finally
        {
            _channelLock.Release();
        }
    }

    public async ValueTask<IChannel> GetChannel(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqConnector));

        if (_channel is { IsHealthy: true })
            return _channel.Channel;

        await InitChannel(cancellationToken).ConfigureAwait(false);

        return _channel?.Channel ?? throw new InvalidOperationException("Channel is not initialized.");
    }

    private async ValueTask InitConnection(CancellationToken cancellationToken)
    {
        if (_connection is not null && _connection.IsOpen)
            return;

        if (_connection is not null && _connectorType == RabbitMqConnectorType.Generic)
        {
            try
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogConnectionDisposeError(_connectionName, ex);
            }
        }

        _connection = _connectorType switch
        {
            RabbitMqConnectorType.Producer => await _connectionProvider.GetSendConnection(cancellationToken).ConfigureAwait(false),
            RabbitMqConnectorType.Consumer => await _connectionProvider.GetReceiveConnection(cancellationToken).ConfigureAwait(false),
            RabbitMqConnectorType.Generic => await _connectionProvider.GetConnection(_customConnectionName, cancellationToken).ConfigureAwait(false),
            _ => throw new InvalidOperationException($"Unknown connector type: {_connectorType}")
        };

        _connectionName = _connection.ClientProvidedName ?? DefaultConnectorName;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await _channelLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_channel is not null)
                await _channel.DisposeAsync().ConfigureAwait(false);

            if (_connectorType == RabbitMqConnectorType.Generic && _connection is not null)
            {
                try
                {
                    if (_connection.IsOpen)
                        await _connection.CloseAsync().ConfigureAwait(false);

                    await _connection.DisposeAsync().ConfigureAwait(false);
                    LogDisposedConnection(_connectionName);
                }
                catch (Exception ex)
                {
                    LogConnectionDisposeError(_connectionName, ex);
                }
            }

            _connectionName = string.Empty;
            _channel = null;
            _connection = null;
            _disposed = true;
        }
        catch (Exception ex)
        {
            LogChannelDisposeError(_connectionName, ex);
        }
        finally
        {
            _channelLock.Release();
        }
    }

    private sealed partial class ManagedChannel : IAsyncDisposable
    {
        private readonly ILogger<ManagedChannel> _logger;
        private bool _disposed;

        public ManagedChannel(IChannel channel, string connectionName, ILogger<ManagedChannel> logger)
        {
            _logger = logger;
            Channel = channel;
            ConnectionName = connectionName;
            Channel.ChannelShutdownAsync += OnChannelShutdown;
        }

        private Task OnChannelShutdown(object sender, ShutdownEventArgs @event)
        {
            var channel = (IChannel)sender;
            var reason = @event.ToString();

            var exception = @event.Exception;
            if (exception is null)
                LogChannelShutdown(channel.ChannelNumber, ConnectionName, reason);
            else
                LogChannelShutdownError(channel.ChannelNumber, ConnectionName, reason, exception);

            return Task.CompletedTask;
        }

        public IChannel Channel { get; }

        public int ChannelNumber => Channel.ChannelNumber;

        public string ConnectionName { get; }

        public bool IsHealthy => !_disposed && Channel.IsOpen;

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            if (Channel.IsOpen)
                await Channel.CloseAsync().ConfigureAwait(false);

            Channel.ChannelShutdownAsync -= OnChannelShutdown;

            await Channel.DisposeAsync().ConfigureAwait(false);

            _disposed = true;
        }

        public static ManagedChannel Create(
            IChannel channel,
            string connectionName,
            ILoggerFactory loggerFactory)
        {
            ArgumentNullException.ThrowIfNull(channel);
            ArgumentException.ThrowIfNullOrEmpty(connectionName);
            ArgumentNullException.ThrowIfNull(loggerFactory);

            var logger = loggerFactory.CreateLogger<ManagedChannel>();

            return new ManagedChannel(
                channel: channel,
                connectionName: connectionName,
                logger: logger);
        }

        [LoggerMessage(
            eventId: 1000,
            level: LogLevel.Information,
            message: "RabbitMQ channel {ChannelNumber} on connection '{ConnectionName}' shutdown: {Reason}")]
        partial void LogChannelShutdown(int channelNumber, string connectionName, string reason);

        [LoggerMessage(
            eventId: 1001,
            level: LogLevel.Error,
            message: "RabbitMQ channel {ChannelNumber} on connection '{ConnectionName}' shutdown with error: {Reason}")]
        partial void LogChannelShutdownError(int channelNumber, string connectionName, string reason, Exception ex);
    }

    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Debug,
        Message = "RabbitMQ {ConnectorType} connector channel {ChannelNumber} on connection '{ConnectionName}' created.")]
    partial void LogChannelCreated(int channelNumber, string connectionName, RabbitMqConnectorType connectorType);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Warning,
        Message = "Error while disposing RabbitMQ connector channel on connection '{ConnectionName}'.")]
    partial void LogChannelDisposeError(string connectionName, Exception exception);

    [LoggerMessage(
        eventId: 1002,
        level: LogLevel.Debug,
        message: "RabbitMQ connection '{ConnectionName}' disposed.")]
    partial void LogDisposedConnection(string connectionName);

    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Warning,
        Message = "Error while disposing RabbitMQ connection for {ConnectionName}.")]
    partial void LogConnectionDisposeError(string connectionName, Exception exception);
}