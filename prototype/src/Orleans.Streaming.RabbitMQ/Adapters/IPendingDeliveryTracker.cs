using Orleans.Streams;
using System.Collections.Concurrent;

namespace Orleans.Streaming.RabbitMQ.Adapters;

/// <summary>
/// Tracks pending deliveries across receivers for a stream provider.
/// Allows adding, removing, and clearing deliveries, especially for failure scenarios.
/// </summary>
public interface IPendingDeliveryTracker
{
    /// <summary>
    /// Adds a pending delivery for a specific queue and sequence token.
    /// </summary>
    void AddPendingDelivery(QueueId queueId, StreamSequenceToken sequenceToken, ulong deliveryTag);

    /// <summary>
    /// Removes a pending delivery (e.g., on successful acknowledgement).
    /// </summary>
    bool TryRemovePendingDelivery(QueueId queueId, StreamSequenceToken sequenceToken, out ulong deliveryTag);

    /// <summary>
    /// Clears a pending delivery for a failed token (e.g., on delivery failure).
    /// Prevents re-queuing on shutdown.
    /// </summary>
    bool ClearPendingDelivery(QueueId queueId, StreamSequenceToken sequenceToken);

    /// <summary>
    /// Find all pending deliveries for a queue (e.g., for shutdown/rejection).
    /// </summary>
    ConcurrentDictionary<StreamSequenceToken, ulong> FindPendingDeliveries(QueueId queueId);

    /// <summary>
    /// Clears all pending deliveries for a queue.
    /// </summary>
    void ClearAllPendingDeliveries(QueueId queueId);
    
}