using Orleans.Streams;
using System.Collections.Concurrent;

namespace Orleans.Streaming.RabbitMQ.Adapters;

internal sealed class PendingDeliveryTracker : IPendingDeliveryTracker
{
    private static readonly ConcurrentDictionary<StreamSequenceToken, ulong> _emptyDeliveries = new();

    private readonly ConcurrentDictionary<QueueId, ConcurrentDictionary<StreamSequenceToken, ulong>> _pendingDeliveries = new();

    public void AddPendingDelivery(QueueId queueId, StreamSequenceToken sequenceToken, ulong deliveryTag)
    {
        var queueDeliveries = _pendingDeliveries.GetOrAdd(queueId, _ => new ConcurrentDictionary<StreamSequenceToken, ulong>());
        queueDeliveries[sequenceToken] = deliveryTag;
    }

    public bool TryRemovePendingDelivery(QueueId queueId, StreamSequenceToken sequenceToken, out ulong deliveryTag)
    {
        if (_pendingDeliveries.TryGetValue(queueId, out var queueDeliveries))
            return queueDeliveries.TryRemove(sequenceToken, out deliveryTag);

        deliveryTag = 0;
        return false;
    }

    public bool ClearPendingDelivery(QueueId queueId, StreamSequenceToken sequenceToken) 
        => _pendingDeliveries.TryGetValue(queueId, out var queueDeliveries)
            ? queueDeliveries.TryRemove(sequenceToken, out _)
            : false;

    public ConcurrentDictionary<StreamSequenceToken, ulong> FindPendingDeliveries(QueueId queueId) 
        => _pendingDeliveries.TryGetValue(queueId, out var queueDeliveries)
            ? queueDeliveries
            : _emptyDeliveries;

    public void ClearAllPendingDeliveries(QueueId queueId) 
        => _pendingDeliveries.TryRemove(queueId, out _);
}