using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Buffers;

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
    /// Serializes the specified events and writes them as a queue message for the given stream using the provided
    /// buffer writer and request context.
    /// </summary>
    /// <typeparam name="T">The type of the events to be serialized and included in the queue message.</typeparam>
    /// <param name="streamId">The identifier of the stream to which the queue message will be associated.</param>
    /// <param name="events">The collection of events to serialize and include in the queue message. Cannot be null.</param>
    /// <param name="requestContext">A dictionary containing contextual information to be included with the queue message. Cannot be null.</param>
    /// <param name="bufferWriter">The buffer writer used to write the serialized queue message. Cannot be null.</param>
    void ToQueueMessage<T>(StreamId streamId, IEnumerable<T> events, Dictionary<string, object> requestContext, IBufferWriter<byte> bufferWriter);

    /// <summary>
    /// Converts a queue message into a batch of messages.
    /// </summary>
    /// <param name="queueMessage">The raw message data from the queue, represented as a read-only memory block of bytes.</param>
    /// <param name="sequenceId"> The sequence identifier for the message, used to maintain order and uniqueness.</param>
    /// <returns>A batch of messages derived from the provided queue message.</returns>
    TMessageBatch FromQueueMessage(ReadOnlyMemory<byte> queueMessage, long sequenceId);
}