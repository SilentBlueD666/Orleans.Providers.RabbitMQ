using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System;

namespace Orleans.Streaming.RabbitMQ.Adapters;

public interface IRabbitMqDataAdapter : IRabbitMqDataAdapter<IBatchContainer>;

/// <summary>
/// Converts event data to and from queue message.
/// </summary>
/// <typeparam name="TQueueMessage">The type of the queue message.</typeparam>
/// <typeparam name="TMessageBatch">The type of the message batch.</typeparam>
public interface IRabbitMqDataAdapter<TMessageBatch>
    where TMessageBatch : class, IBatchContainer
{
    /// <summary>
    /// Creates a cloud queue message from stream event data.
    /// </summary>
    /// <typeparam name="T">The stream event type.</typeparam>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The events.</param>
    /// <param name="token">The token.</param>
    /// <param name="requestContext">The request context.</param>
    /// <returns>A read-only memory block containing the serialized queue message.</returns>
    ReadOnlyMemory<byte> ToQueueMessage<T>(StreamId streamId, IEnumerable<T> events, Dictionary<string, object> requestContext);

    /// <summary>
    /// Converts a queue message into a batch of messages.
    /// </summary>
    /// <param name="queueMessage">The raw message data from the queue, represented as a read-only memory block of bytes.</param>
    /// <param name="sequenceId"> The sequence identifier for the message, used to maintain order and uniqueness.</param>
    /// <returns>A batch of messages derived from the provided queue message.</returns>
    TMessageBatch FromQueueMessage(ReadOnlyMemory<byte> queueMessage, long sequenceId);
}