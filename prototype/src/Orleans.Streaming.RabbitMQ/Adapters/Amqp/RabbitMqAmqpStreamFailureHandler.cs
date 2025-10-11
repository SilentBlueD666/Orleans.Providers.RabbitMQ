using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Adapters.Amqp;

/// <summary>
/// This handler will look up the pending delivery for the given stream and sequence token, and reject it.
/// </summary>
/// <remarks>
/// Routing Dead-Lettered Messages - <see href="https://www.rabbitmq.com/docs/dlx"/> configure a dead-letter exchange and queue as a policy in RabbitMQ management UI or via rabbitmqctl. 
/// In this handler we simply reject the message, the message will be lost unless you have configured dead-lettering policy in RabbitMQ.
/// </remarks>
/// <param name="pendingDeliveryTracker"></param>
/// <param name="rabbitMqQueueProvider"></param>
/// <param name="connectorFactory"></param>
/// <param name="rabbitMqOptions"></param>
/// <param name="loggerFactory"></param>
internal sealed partial class RabbitMqAmqpStreamFailureHandler(
    IPendingDeliveryTracker pendingDeliveryTracker,
    IRabbitMqQueueProvider rabbitMqQueueProvider,
    IRabbitMqConnectorFactory connectorFactory,
    RabbitMqOptions rabbitMqOptions,
    ILoggerFactory loggerFactory)
    :
    IStreamFailureHandler,
    IAsyncDisposable
{
    private readonly ILogger<RabbitMqAmqpStreamFailureHandler> _logger = loggerFactory.CreateLogger<RabbitMqAmqpStreamFailureHandler>();
    private readonly IPendingDeliveryTracker _pendingDeliveryTracker = pendingDeliveryTracker;
    private readonly IRabbitMqQueueProvider _rabbitMqQueueProvider = rabbitMqQueueProvider;
    private readonly RabbitMqOptions _rabbitMqOptions = rabbitMqOptions;
    private readonly IRabbitMqConnector _consumerConnector = connectorFactory.CreateConsumerConnector();

    public bool ShouldFaultSubsriptionOnError => _rabbitMqOptions.ShouldFaultSubsriptionOnError;

    public async Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        await RejectPendingDelivery(subscriptionId, streamIdentity, sequenceToken).ConfigureAwait(false);
        LogDeliveryFailure(subscriptionId, streamIdentity, sequenceToken);
    }

    public async Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        await RejectPendingDelivery(subscriptionId, streamIdentity, sequenceToken).ConfigureAwait(false);
        LogSubscriptionFailure(subscriptionId, streamIdentity, sequenceToken);
    }

    private async ValueTask RejectPendingDelivery(GuidId subscriptionId, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        var queueId = _rabbitMqQueueProvider.GetQueueForStream(streamIdentity);
        if (_pendingDeliveryTracker.TryRemovePendingDelivery(queueId, sequenceToken, out var deliveryTag))
        {
            var channel = await _consumerConnector.GetChannel().ConfigureAwait(false);
            await channel.BasicRejectAsync(deliveryTag).ConfigureAwait(false);
        }
        else
            LogPendingDeliveryNotFound(subscriptionId, streamIdentity, sequenceToken);
    }

    public async ValueTask DisposeAsync() => await _consumerConnector.DisposeAsync().ConfigureAwait(false);

    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Warning,
        Message = "Delivery failure for stream {StreamId} with token {SequenceToken} on subscription {SubscriptionId}. Message has been rejected.")]
    partial void LogDeliveryFailure(GuidId subscriptionId, StreamId streamId, StreamSequenceToken sequenceToken);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Warning,
        Message = "Failed to find pending delivery for stream {StreamId} with token {SequenceToken} on subscription {SubscriptionId}. It may have already been acknowledged or cleared.")]
    partial void LogPendingDeliveryNotFound(GuidId subscriptionId, StreamId streamId, StreamSequenceToken sequenceToken);

    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Warning,
        Message = "Subscription failure for stream {StreamId} with token {SequenceToken} on subscription {SubscriptionId}. Message has been rejected.")]
    partial void LogSubscriptionFailure(GuidId subscriptionId, StreamId streamId, StreamSequenceToken sequenceToken);
}
