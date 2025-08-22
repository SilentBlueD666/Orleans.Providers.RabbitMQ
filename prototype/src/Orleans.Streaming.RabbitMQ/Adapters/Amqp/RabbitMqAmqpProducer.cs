using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using RabbitMQ.Client;
using System.Data.Common;
using System.Threading;
using System.Threading.Channels;
using System.Xml.Linq;

namespace Orleans.Streaming.RabbitMQ.Adapters.Amqp;

internal sealed partial class RabbitMqAmqpProducer(
    string providerName,
    string queueName,
    IRabbitMqConnectorFactory connectorFactory,
    RabbitMqOptions options,
    IRabbitMqDataAdapter dataAdapter,
    TimeProvider timeProvider,
    ILoggerFactory loggerFactory)
    :
    IAsyncDisposable
{
    private readonly string _providerName = providerName;
    private readonly string _queueName = queueName;
    private readonly IRabbitMqConnector _producerConnector = connectorFactory.CreateProducerConnector();
    private readonly RabbitMqOptions _options = options;
    private readonly IRabbitMqDataAdapter _dataAdapter = dataAdapter;
    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly ILogger<RabbitMqAmqpProducer> _logger = loggerFactory.CreateLogger<RabbitMqAmqpProducer>();
    private readonly SemaphoreSlim _publishLock = new(1, 1);

    private bool _disposed;
    private bool _initialized;

    public async ValueTask Initialize()
    {
        if (_initialized)
            return;

        if (_options is { QueueDeclaration: QueueDeclarationMode.OnDemand })
        {
            var channel = await _producerConnector.GetChannel().ConfigureAwait(false);
            await channel.ExchangeAndQueueDeclareAsync(_queueName, _options).ConfigureAwait(false);
        }
        else
            await _producerConnector.InitChannel().ConfigureAwait(false);

        _initialized = true;
    }

    public async ValueTask SendMessage<T>(StreamId streamId, T @event, Dictionary<string, object> requestContext)
        => await SendMessage(streamId, [@event], requestContext).ConfigureAwait(false);

    public async ValueTask SendMessage<T>(StreamId streamId, IEnumerable<T> events, Dictionary<string, object> requestContext)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqAmqpProducer));

        if (!_initialized)
            await Initialize().ConfigureAwait(false);

        var timestamp = _timeProvider.GetUtcNow();

#if NET9_0_OR_GREATER
        var messageId = Guid.CreateVersion7(timestamp);
#else
        // For .NET 8 and earlier, we use a standard GUID
        var messageId = Guid.NewGuid();
#endif
        var messageBody = _dataAdapter.ToQueueMessage(streamId, events, requestContext);
        var properties = new BasicProperties()
        {
            Headers = new Dictionary<string, object?>(1)
            {
                { HeaderConstants.StreamId, streamId.ToString() }
            },
            MessageId = messageId.ToString(),
            Persistent = true,
            Timestamp = new AmqpTimestamp(timestamp.ToUnixTimeMilliseconds()),
        };

        var channel = await _producerConnector.GetChannel().ConfigureAwait(false);
        await _publishLock.WaitAsync();
        try
        {
            await channel.BasicPublishAsync(
                exchange: _options.ExchangeName,
                mandatory: true,
                routingKey: _queueName,
                basicProperties: properties,
                body: messageBody
            );
        }
        finally
        {
            _publishLock.Release();
        }

        LogPublishedMessage(_providerName, messageId, _queueName, streamId);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await _producerConnector.DisposeAsync().ConfigureAwait(false);

        _disposed = true;
    }

    public static RabbitMqAmqpProducer Create(
        string providerName,
        QueueId queueId,
        IRabbitMqConnectorFactory connectorFactory,
        RabbitMqOptions options,
        IRabbitMqDataAdapter dataAdapter,
        TimeProvider timeProvider,
        ILoggerFactory loggerFactory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        return new RabbitMqAmqpProducer(
            providerName: providerName,
            queueName: queueId.ToString(),
            connectorFactory: connectorFactory,
            options: options,
            dataAdapter: dataAdapter,
            timeProvider: timeProvider,
            loggerFactory: loggerFactory);
    }

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Debug,
        Message = "Provider {ProviderName} published message {MessageId} to queue {QueueName} for StreamId: {StreamId}.")]
    partial void LogPublishedMessage(string providerName, Guid messageId, string queueName, StreamId streamId);
}