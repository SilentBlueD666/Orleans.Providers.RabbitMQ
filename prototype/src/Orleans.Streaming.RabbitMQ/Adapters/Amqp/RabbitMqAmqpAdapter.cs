using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Adapters.Amqp;

/// <summary>
/// RabbitMQ stream queue storage adapter.
/// </summary>
/// <remarks>
/// This is an abstraction layer that hides the implementation details of the underlying queuing system.
/// </remarks>
internal partial class RabbitMqAmqpAdapter(
    string providerName,
    IRabbitMqConnectorFactory connectorFactory,
    IRabbitMqDataAdapter dataAdapter,
    IRabbitMqQueueProvider queueProvider,
    RabbitMqOptions options,
    TimeProvider timeProvider,
    ILoggerFactory loggerFactory)
    :
    IQueueAdapter,
    IAsyncDisposable
{
    private readonly IRabbitMqConnectorFactory _connectorFactory = connectorFactory;
    private readonly IRabbitMqDataAdapter _dataAdapter = dataAdapter;
    private readonly IRabbitMqQueueProvider _queueProvider = queueProvider;
    private readonly RabbitMqOptions _options = options;
    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly ILoggerFactory _loggerFactory = loggerFactory;
    private readonly ILogger<RabbitMqAmqpAdapter> _logger = loggerFactory.CreateLogger<RabbitMqAmqpAdapter>();

    private readonly ConcurrentDictionary<QueueId, RabbitMqAmqpProducer> _producers = new();

    public string Name => providerName;

    public bool IsRewindable => false; // RabbitMQ AMQP Queues do not support rewind.

    public StreamProviderDirection Direction => _options.Mode;

    public async ValueTask InitializeAsync()
    {
        if (_options is not { QueueDeclaration: QueueDeclarationMode.AtStartup })
            return;

        await using var connector = _connectorFactory.CreateConnector($"{Name}_Declarer");
        var channel = await connector.GetChannel().ConfigureAwait(false);
        await channel.ExchangeDeclareAsync(_options).ConfigureAwait(false);

        var queues = _queueProvider.GetAllQueues();
        foreach (var queueId in queues)
        {
            var queueName = queueId.ToString();
            await channel.QueueDeclareAsync(queueName, _options).ConfigureAwait(false);
        }
    }

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        => RabbitMqAmqpAdapterReceiver.Create(
            providerName: Name,
            queueName: _queueProvider.GetQueueName(queueId),
            options: _options,
            connectorFactory: _connectorFactory,
            dataAdapter: _dataAdapter,
            loggerFactory: _loggerFactory);

    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
    {
        if (token is not null) throw new ArgumentException("RabbitMq stream provider does not support non-null StreamSequenceToken.", nameof(token));

        var queueId = _queueProvider.GetQueueForStream(streamId);

        var producer = _producers.GetOrAdd(
            key: queueId,
            valueFactory: id => RabbitMqAmqpProducer.Create(
                providerName: Name,
                queueId: id,
                connectorFactory: _connectorFactory,
                options: _options,
                dataAdapter: _dataAdapter,
                timeProvider: _timeProvider,
                loggerFactory: _loggerFactory));

        if (_options.SendAsBatch)
            await producer.SendMessage(streamId, events, requestContext).ConfigureAwait(false);
        else
        {
            foreach (var @event in events)
            {
                await producer.SendMessage(streamId, @event, requestContext).ConfigureAwait(false);
            }
        }
    }

    public async ValueTask Shutdown()
    {
        if (_producers.Count == 0)
            return;

        var tasks = _producers
            .Values
            .Select(async producer =>
            {
                await producer.DisposeAsync().ConfigureAwait(false);
            });

        await Task.WhenAll(tasks).ConfigureAwait(false);

        _producers.Clear();
    }

    public async ValueTask DisposeAsync() => await Shutdown().ConfigureAwait(false);
}
