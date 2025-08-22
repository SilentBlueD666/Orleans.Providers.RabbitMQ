using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Adapters.Amqp;

internal sealed class RabbitMqAmqpQueueProvider : IRabbitMqQueueProvider
{
    private readonly IStreamQueueMapper _streamQueueMapper;
    private readonly Func<QueueId, string> _queueNameResolver;

    private RabbitMqAmqpQueueProvider(IStreamQueueMapper streamQueueMapper)
    {
        _streamQueueMapper = streamQueueMapper ?? throw new ArgumentNullException(nameof(streamQueueMapper));
        _queueNameResolver = streamQueueMapper is HashRingBasedPartitionedStreamQueueMapper partitionedStreamQueueMapper
            ? new Func<QueueId, string>(queueId => partitionedStreamQueueMapper.QueueToPartition(queueId))
            : new Func<QueueId, string>(queueId => queueId.ToString());
    }

    public IEnumerable<QueueId> GetAllQueues() 
        => _streamQueueMapper.GetAllQueues();

    public QueueId GetQueueForStream(StreamId streamId) 
        => _streamQueueMapper.GetQueueForStream(streamId);

    public string GetQueueName(QueueId queueId) 
        => _queueNameResolver.Invoke(queueId);

    public IStreamQueueMapper GetStreamQueueMapper() 
        => _streamQueueMapper;

    public static IRabbitMqQueueProvider Create(string providerName, RabbitMqOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(options);

        var queueNamePrefix = string.IsNullOrWhiteSpace(options.QueueNamePrefix) ? providerName : options.QueueNamePrefix;
        var streamQueueMapper = options.QueueNames is null or { Count: 0 }
            ? new HashRingBasedStreamQueueMapper(
                options: new HashRingStreamQueueMapperOptions()
                {
                    TotalQueueCount = options.NumberOfQueues
                },
                queueNamePrefix: queueNamePrefix)
            : new HashRingBasedPartitionedStreamQueueMapper(
                partitionIds: options.QueueNames,
                queueNamePrefix: queueNamePrefix);

        return new RabbitMqAmqpQueueProvider(streamQueueMapper);
    }
}
