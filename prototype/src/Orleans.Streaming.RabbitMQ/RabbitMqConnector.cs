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
    string? customConnectionName = null) : IRabbitMqConnector
{
    private readonly ILogger<RabbitMqConnector> _logger = loggerFactory.CreateLogger<RabbitMqConnector>();
    private readonly IRabbitMqConnectionProvider _connectionProvider = connectionProvider;
    private readonly RabbitMqConnectorType _connectorType = connectorType;
    private readonly RabbitMqOptions _options = options;
    private readonly string? _customConnectionName = customConnectionName;

    private bool _disposed;
    private IChannel? _channel;
    private SemaphoreSlim _channelLock = new(1, 1);
    private string _connectionName = string.Empty;
    private IConnection? _connection;

    public async ValueTask InitChannel(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqConnector));

        await _channelLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_channel is { IsOpen: true })
                return;

            if (_channel is not null)
            {
                _channel.ChannelShutdownAsync -= ChannelShutdown;
                _channel.CallbackExceptionAsync -= ChannelException;
                await _channel.DisposeAsync().ConfigureAwait(false);
            }

            await InitConnection(cancellationToken).ConfigureAwait(false);
            if (_connection is null || !_connection.IsOpen)
                throw new InvalidOperationException("Connection is not initialized or not open.");

            _channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            _channel.ChannelShutdownAsync += ChannelShutdown;
            _channel.CallbackExceptionAsync += ChannelException;

            LogConnectorCreated(_channel.ChannelNumber, _connectionName, _connectorType);

            await OnChannelCreated(_channel, cancellationToken).ConfigureAwait(false);
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

        if (_channel is { IsOpen: true })
            return _channel;

        await InitChannel(cancellationToken).ConfigureAwait(false);

        return _channel ?? throw new InvalidOperationException("Channel is not initialized.");
    }

    private async ValueTask InitConnection(CancellationToken cancellationToken)
    {
        if (_connection is not null && _connection.IsOpen)
            return;

        if (_connectorType == RabbitMqConnectorType.Generic && _connection is not null)
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

        _connectionName = _connection.ClientProvidedName ?? "Orleans-Streaming-RabbitMQ-Connector";
    }

    private async ValueTask OnChannelCreated(IChannel channel, CancellationToken cancellationToken)
    {
        if (_connectorType == RabbitMqConnectorType.Consumer && _options.PrefetchCount > 0)
            await channel.BasicQosAsync(0, (ushort)_options.PrefetchCount, false, cancellationToken).ConfigureAwait(false);
    }

    private Task ChannelException(object sender, CallbackExceptionEventArgs @event)
    {
        var channel = (IChannel)sender;
        LogChannelError(channel.ChannelNumber, _connectionName, @event.Exception.Message, @event.Exception);
        return Task.CompletedTask;
    }

    private Task ChannelShutdown(object sender, ShutdownEventArgs @event)
    {
        var channel = (IChannel)sender;
        var replyText = @event.ReplyText ?? "No reply text provided";
        LogChannelShutdown(channel.ChannelNumber, _connectionName, replyText);
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await _channelLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_channel is not null)
            {
                _channel.ChannelShutdownAsync -= ChannelShutdown;
                _channel.CallbackExceptionAsync -= ChannelException;

                if (_channel.IsOpen)
                    await _channel.CloseAsync().ConfigureAwait(false);

                await _channel.DisposeAsync().ConfigureAwait(false);
            }

            if (_connectorType == RabbitMqConnectorType.Generic && _connection is not null)
            {
                try
                {
                    if (_connection.IsOpen)
                        await _connection.CloseAsync().ConfigureAwait(false);

                    await _connection.DisposeAsync().ConfigureAwait(false);
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

    [LoggerMessage(EventId = 1001, Level = LogLevel.Debug, Message = "RabbitMQ {ConnectorType} connector channel {ChannelNumber} for {ConnectionName} created.")]
    partial void LogConnectorCreated(int channelNumber, string connectionName, RabbitMqConnectorType connectorType);

    [LoggerMessage(EventId = 1002, Level = LogLevel.Debug, Message = "RabbitMQ connector channel {ChannelNumber} for {ConnectionName} was shut down, server reply: {ServerReply}")]
    partial void LogChannelShutdown(int channelNumber, string connectionName, string serverReply);

    [LoggerMessage(EventId = 1003, Level = LogLevel.Warning, Message = "RabbitMQ connector channel {ChannelNumber} for {ConnectionName} encountered an error: {ExceptionMessage}")]
    partial void LogChannelError(int channelNumber, string connectionName, string exceptionMessage, Exception exception);

    [LoggerMessage(EventId = 1004, Level = LogLevel.Warning, Message = "Error while disposing RabbitMQ connector channel for {ConnectionName}.")]
    partial void LogChannelDisposeError(string connectionName, Exception exception);

    [LoggerMessage(EventId = 1005, Level = LogLevel.Warning, Message = "Error while disposing RabbitMQ connection for {ConnectionName}.")]
    partial void LogConnectionDisposeError(string connectionName, Exception exception);
}