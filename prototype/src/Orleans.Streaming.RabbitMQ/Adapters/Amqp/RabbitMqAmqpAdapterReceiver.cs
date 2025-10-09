using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Adapters.Amqp;

internal sealed partial class RabbitMqAmqpAdapterReceiver : IQueueAdapterReceiver
{
    private const int ReceiverShutdown = 0;
    private const int ReceiverRunning = 1;
    private static readonly IBatchContainer[] _emptyMessageBatch = Array.Empty<IBatchContainer>();

    private readonly string _providerName;
    private readonly string _queueName;
    private readonly IRabbitMqConnector _consumerConnector;
    private readonly RabbitMqOptions _options;
    private readonly IRabbitMqDataAdapter _dataAdapter;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<StreamSequenceToken, ulong> _pendingDeliveries = [];

    private int _receiverState = ReceiverShutdown;
    private long _messagesConsumedCount;
    private long _messagesDeliveredCount;
    private long _messageAcknowledgedCount;
    private long _sequenceNumber = -1;

    private bool _initialized;

    private RabbitMqAmqpAdapterReceiver(
        string providerName,
        string queueName,
        IRabbitMqConnectorFactory connectorFactory,
        RabbitMqOptions options,
        IRabbitMqDataAdapter dataAdapter,
        ILoggerFactory loggerFactory)
    {
        if (string.IsNullOrEmpty(queueName))
            throw new ArgumentException($"'{nameof(queueName)}' cannot be null or empty.", nameof(queueName));

        if (loggerFactory is null)
            throw new ArgumentNullException(nameof(loggerFactory));

        _providerName = providerName;
        _queueName = queueName;
        _consumerConnector = connectorFactory.CreateConsumerConnector();
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _dataAdapter = dataAdapter ?? throw new ArgumentNullException(nameof(dataAdapter));
        _logger = loggerFactory.CreateLogger<RabbitMqAmqpAdapterReceiver>();
    }

    public async Task Initialize(TimeSpan timeout)
    {
        if (ReceiverRunning == Interlocked.Exchange(ref _receiverState, ReceiverRunning))
        {
            LogReceiverAlreadyInitialized(_providerName, _queueName);
            return;
        }

        LogStartingReceiver(_providerName, _queueName);

        try
        {
            using var cancellationTokenSource = new CancellationTokenSource(timeout);
            var cancellationToken = cancellationTokenSource.Token;

            if (_options is { QueueDeclaration: QueueDeclarationMode.OnDemand })
            {
                var channel = await _consumerConnector.GetChannel(cancellationToken).ConfigureAwait(false);
                await channel.ExchangeAndQueueDeclareAsync(_queueName, _options, cancellationToken).ConfigureAwait(false);
            }
            else
                await _consumerConnector.InitChannel(cancellationToken).ConfigureAwait(false);

            LogInitializedReceiver(_providerName, _queueName);
        }
        catch (Exception ex)
        {
            LogReceiverInitializationFailed(_providerName, _queueName, ex);
            throw;
        }

        _initialized = true;
    }

    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        if (!_initialized || _receiverState == ReceiverShutdown)
            return _emptyMessageBatch;

        var maxConsumerMessages = _options.MaxConsumerMessages;
        var messagesToConsume = maxConsumerMessages > 0
            ? Math.Min(maxCount, maxConsumerMessages)
            : maxCount;

        const int initialBufferSize = 32;
        var buffer = messagesToConsume <= initialBufferSize
            ? new IBatchContainer[messagesToConsume]
            : new IBatchContainer[initialBufferSize];

        var channel = await _consumerConnector.GetChannel().ConfigureAwait(false);
        int count = 0;

        for (int i = 0; i < messagesToConsume; i++)
        {
            if (_receiverState == ReceiverShutdown)
                break;

            var result = await channel.BasicGetAsync(_queueName, autoAck: false);
            if (result is null)
            {
                LogNoMoreMessagesInQueue(_queueName, count, _providerName);
                break;
            }

            //TODO: DLQ logic ??? Currently we just reject/discard the message if we can't de-serialise or process it.
            try
            {
                //TODO: Should we be using our own sequence number or use delivery tag '(long)result.DeliveryTag' from RabbitMQ? https://www.rabbitmq.com/docs/confirms#consumer-acks-delivery-tags
                var sequenceId = Interlocked.Add(ref _sequenceNumber, 1);
                var batchContainer = _dataAdapter.FromQueueMessage(queueMessage: result.Body, sequenceId: sequenceId);
                if (batchContainer is not null)
                {
                    if (count >= buffer.Length)
                        Array.Resize(ref buffer, Math.Min(buffer.Length * 2, messagesToConsume));

                    buffer[count++] = batchContainer;
                    _pendingDeliveries.TryAdd(batchContainer.SequenceToken, result.DeliveryTag);

                    LogRetrievedMessage(_queueName, batchContainer.StreamId, _providerName, batchContainer.SequenceToken);
                }
                else
                {
                    LogReceivedNullMessage(_providerName, _queueName);
                    await channel.BasicRejectAsync(result.DeliveryTag, requeue: false).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                LogErrorProcessingMessage(_providerName, _queueName, ex);
                await channel.BasicRejectAsync(result.DeliveryTag, requeue: false).ConfigureAwait(false);
            }
        }

        if (count > 0)
            Interlocked.Add(ref _messagesConsumedCount, count);

        return count switch
        {
            0 => _emptyMessageBatch,
            _ when count == buffer.Length => buffer,
            _ => buffer[..count]
        };
    }

    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        var messagesDeliveredCount = messages.Count;
        if (messagesDeliveredCount == 0)
            return;

        Interlocked.Add(ref _messagesDeliveredCount, messagesDeliveredCount);

        try
        {
            var channel = await _consumerConnector.GetChannel().ConfigureAwait(false);
            int acknowledgedCount = 0;
            foreach (var message in messages)
            {
                if (_pendingDeliveries.TryRemove(message.SequenceToken, out ulong deliveryTag))
                {
                    await channel.BasicAckAsync(deliveryTag).ConfigureAwait(false);
                    acknowledgedCount++;
                }
            }

            if (acknowledgedCount > 0)
            {
                Interlocked.Add(ref _messageAcknowledgedCount, acknowledgedCount);
                LogAcknowledgedMessages(_providerName, acknowledgedCount, _queueName);
            }
        }
        catch (Exception ex)
        {
            LogErrorAcknowledgingMessages(_providerName, _queueName, ex);
            //TODO: Messages will remain in the pending dictionary and on the queue, need to handle re-queueing or poison queue logic...
        }
    }

    public async Task Shutdown(TimeSpan timeout)
    {
        if (ReceiverShutdown == Interlocked.Exchange(ref _receiverState, ReceiverShutdown))
            return;

        if (_pendingDeliveries.IsEmpty)
        {
            await _consumerConnector.DisposeAsync().ConfigureAwait(false);
            return;
        }

        using var cancellationTokenSource = new CancellationTokenSource(timeout);
        var cancellationToken = cancellationTokenSource.Token;

        var deliveries = _pendingDeliveries.ToArray();

        try
        {
            var channel = await _consumerConnector.GetChannel(cancellationToken).ConfigureAwait(false);
            foreach (var delivery in deliveries)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                if (_pendingDeliveries.TryRemove(delivery.Key, out ulong deliveryTag))
                    await channel
                        .BasicRejectAsync(
                            deliveryTag: deliveryTag,
                            requeue: true,
                            cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            LogErrorRejectingMessages(_providerName, _queueName, ex);
        }

        await _consumerConnector.DisposeAsync().ConfigureAwait(false);
    }

    public static IQueueAdapterReceiver Create(
        string providerName,
        string queueName,
        RabbitMqOptions options,
        IRabbitMqConnectorFactory connectorFactory,
        IRabbitMqDataAdapter dataAdapter,
        ILoggerFactory loggerFactory)
            => new RabbitMqAmqpAdapterReceiver(
                providerName: providerName,
                queueName: queueName,
                connectorFactory: connectorFactory,
                options: options,
                dataAdapter: dataAdapter,
                loggerFactory: loggerFactory);

    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Debug,
        Message = "Provider {ProviderName} starting RabbitMQ receiver for queue '{QueueName}'")]
    partial void LogStartingReceiver(string providerName, string queueName);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Information,
        Message = "Provider {ProviderName} initialised RabbitMQ receiver for queue '{QueueName}'")]
    partial void LogInitializedReceiver(string providerName, string queueName);

    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Warning,
        Message = "Another initialization for this receiver instance is already in progress for provider {ProviderName} on queue '{QueueName}', cancelling")]
    partial void LogReceiverAlreadyInitialized(string providerName, string queueName);

    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Error,
        Message = "Provider {ProviderName} receiver initialisation failed for queue '{QueueName}'.")]
    partial void LogReceiverInitializationFailed(string providerName, string queueName, Exception exception);

    [LoggerMessage(
        EventId = 1004,
        Level = LogLevel.Trace,
        Message = "No more messages in queue '{QueueName}' after retrieving {Count} for provider {ProviderName}")]
    partial void LogNoMoreMessagesInQueue(string queueName, int count, string providerName);

    [LoggerMessage(
        EventId = 1005,
        Level = LogLevel.Debug,
        Message = "Retrieved message from queue '{QueueName}' for StreamId: {StreamId} in provider {ProviderName}. Sequence token: {SequenceToken}")]
    partial void LogRetrievedMessage(string queueName, StreamId streamId, string providerName, StreamSequenceToken sequenceToken);

    [LoggerMessage(
        EventId = 1006,
        Level = LogLevel.Warning,
        Message = "Provider {ProviderName} failed to de-serialise the message from queue '{QueueName}', The message will be discarded.")]
    partial void LogReceivedNullMessage(string providerName, string queueName);

    [LoggerMessage(
        EventId = 1007,
        Level = LogLevel.Error,
        Message = "Provider {ProviderName} error processing message from queue '{QueueName}', The message will be discarded.")]
    partial void LogErrorProcessingMessage(string providerName, string queueName, Exception exception);

    [LoggerMessage(
    EventId = 1008,
    Level = LogLevel.Debug,
    Message = "Provider {ProviderName} has acknowledged {Count} messages from queue '{QueueName}'")]
    partial void LogAcknowledgedMessages(string providerName, int count, string queueName);

    [LoggerMessage(
        EventId = 1009,
        Level = LogLevel.Error,
        Message = "Provider {ProviderName} error acknowledging messages from queue '{QueueName}'")]
    partial void LogErrorAcknowledgingMessages(string providerName, string queueName, Exception exception);

    [LoggerMessage(
        EventId = 1010,
        Level = LogLevel.Error,
        Message = "Provider {ProviderName} error rejecting messages from queue '{QueueName}'")]
    partial void LogErrorRejectingMessages(string providerName, string queueName, Exception exception);
}
