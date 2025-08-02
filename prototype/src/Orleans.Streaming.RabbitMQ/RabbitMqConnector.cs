using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orleans.Streaming.RabbitMQ;

internal abstract partial class RabbitMqConnector(ILoggerFactory loggerFactory) : IRabbitMqConnector
{
    private readonly ILogger<RabbitMqProducerConnector> _logger = loggerFactory.CreateLogger<RabbitMqProducerConnector>();

    private bool _disposed;
    private IChannel? _channel;
    private SemaphoreSlim _channelLock = new(1, 1);
    private string _connectionName = string.Empty;

    public virtual async ValueTask InitChannel(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqProducerConnector));

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

            var connection = await GetConnection(cancellationToken).ConfigureAwait(false);
            _channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            _channel.ChannelShutdownAsync += ChannelShutdown;
            _channel.CallbackExceptionAsync += ChannelException;

            _connectionName = connection.ClientProvidedName ?? "Orleans-Streaming-RabbitMQ-Connector";
            LogProducerConnectorCreated(_channel.ChannelNumber, _connectionName);

            await OnChannelCreated(_channel, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _channelLock.Release();
        }
    }

    public virtual async ValueTask<IChannel> GetChannel(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqProducerConnector));

        if (_channel is { IsOpen: true })
            return _channel;

        await InitChannel(cancellationToken).ConfigureAwait(false);

        return _channel ?? throw new InvalidOperationException("Channel is not initialized.");
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

    protected abstract ValueTask<IConnection> GetConnection(CancellationToken cancellationToken = default);

    protected virtual ValueTask OnChannelCreated(IChannel channel, CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;

    private async ValueTask ClearChannel()
    {
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

            _connectionName = string.Empty;
            _channel = null;
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

    public virtual async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await ClearChannel().ConfigureAwait(false);
        _disposed = true;
    }

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Debug,
        Message = "RabbitMQ connector channel {ChannelNumber} for {ConnectionName} created.")]
    partial void LogProducerConnectorCreated(int channelNumber, string connectionName);

    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Debug,
        Message = "RabbitMQ connector channel {ChannelNumber} for {ConnectionName} was shut down, server reply: {ServerReply}")]
    partial void LogChannelShutdown(int channelNumber, string connectionName, string serverReply);

    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Warning,
        Message = "RabbitMQ connector channel {ChannelNumber} for {ConnectionName} encountered an error: {ExceptionMessage}")]
    partial void LogChannelError(int channelNumber, string connectionName, string exceptionMessage, Exception exception);

    [LoggerMessage(
        EventId = 1004,
        Level = LogLevel.Warning,
        Message = "Error while disposing RabbitMQ connector channel for {ConnectionName}.")]
    partial void LogChannelDisposeError(string connectionName, Exception exception);
}