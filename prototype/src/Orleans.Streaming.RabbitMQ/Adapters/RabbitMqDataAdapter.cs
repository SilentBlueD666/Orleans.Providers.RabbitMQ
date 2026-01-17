using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Adapters;

internal sealed class RabbitMqDataAdapter(Serializer serializer, TimeProvider timeProvider) : IRabbitMqDataAdapter
{
    private readonly Serializer<RabbitMqBatchContainer> _serializer = serializer.GetSerializer<RabbitMqBatchContainer>();
    private readonly TimeProvider _timeProvider = timeProvider;

    public IBatchContainer FromQueueMessage(ReadOnlyMemory<byte> queueMessage, long sequenceId)
    {
        var message = _serializer.Deserialize(queueMessage);
        message.RealSequenceToken = new EventSequenceTokenV2(sequenceId);
        return message;
    }

    public void ToQueueMessage<T>(StreamId streamId, IEnumerable<T> events, Dictionary<string, object> requestContext, IBufferWriter<byte> bufferWriter)
    {
        var container = new RabbitMqBatchContainer(
            streamId: streamId,
            events: events.Cast<object>().ToList(),
            requestContext: requestContext ?? [],
            enqueueTime: _timeProvider.GetUtcNow().DateTime);

        _serializer.Serialize(container, bufferWriter);
    }
}
