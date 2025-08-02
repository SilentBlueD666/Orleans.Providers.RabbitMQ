using Orleans.Streams;

namespace Orleans.Streaming.RabbitMQ.Adapters;

public interface IRabbitMqQueueProvider
{
    IStreamQueueMapper GetStreamQueueMapper();

    IEnumerable<QueueId> GetAllQueues();

    QueueId GetQueueForStream(StreamId streamId);

    string GetQueueName(QueueId queueId);
}
