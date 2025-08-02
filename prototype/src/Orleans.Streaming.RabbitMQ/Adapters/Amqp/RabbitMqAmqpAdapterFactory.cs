using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Orleans.Streaming.RabbitMQ.Adapters.Amqp;

internal sealed class RabbitMqAmqpAdapterFactory(
    string providerName,
    IRabbitMqConnectorFactory connectorFactory,
    RabbitMqOptions options,
    SimpleQueueCacheOptions cacheOptions,
    HashRingStreamQueueMapperOptions hashRingStreamQueueOptions,
    IRabbitMqDataAdapter dataAdapter,
    TimeProvider timeProvider,
    ILoggerFactory loggerFactory)
    :
    IQueueAdapterFactory
{

    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly IRabbitMqQueueProvider _queueProvider = RabbitMqAmqpQueueProvider.Create(providerName, options, hashRingStreamQueueOptions);
    private readonly SimpleQueueAdapterCache _adapterCache = new(cacheOptions, providerName, loggerFactory);

    public Func<QueueId, Task<IStreamFailureHandler>>? StreamFailureHandlerFactory { private get; set; }

    public void Initialize()
    {
        StreamFailureHandlerFactory = StreamFailureHandlerFactory 
            ?? (_ => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler()));
    }

    public async Task<IQueueAdapter> CreateAdapter()
    {
        var adapter = new RabbitMqAmqpAdapter(
            providerName: providerName,
            connectorFactory: connectorFactory,
            dataAdapter: dataAdapter,
            queueProvider: _queueProvider,
            options: options,
            timeProvider: _timeProvider,
            loggerFactory: loggerFactory);

        await adapter.InitializeAsync().ConfigureAwait(false);

        return adapter;
    }

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        => StreamFailureHandlerFactory?.Invoke(queueId) 
            ?? throw new InvalidOperationException("StreamFailureHandlerFactory is not set. Please set it before calling GetDeliveryFailureHandler.");

    public IQueueAdapterCache GetQueueAdapterCache()
        => _adapterCache;

    public IStreamQueueMapper GetStreamQueueMapper()
        => _queueProvider.GetStreamQueueMapper();

    public static IQueueAdapterFactory Create(IServiceProvider serviceProvider, string providerName)
    {
        var rabbitMqOptions = serviceProvider.GetOptionsByName<RabbitMqOptions>(providerName);
        var cacheOptions = serviceProvider.GetOptionsByName<SimpleQueueCacheOptions>(providerName);

        var connectionProvider = serviceProvider.GetRequiredKeyedService<IRabbitMqConnectionProvider>(providerName);

        var hashRingStreamQueueMapperOptions = serviceProvider.GetOptionsByName<HashRingStreamQueueMapperOptions>(providerName)
            ?? new HashRingStreamQueueMapperOptions();

        var connectorFactory = serviceProvider.GetKeyedService<IRabbitMqConnectorFactory>(providerName)
            ?? new RabbitMqConnectorFactory(
                connectionProvider,
                rabbitMqOptions,
                serviceProvider.GetRequiredService<ILoggerFactory>());

        var factory = ActivatorUtilities.CreateInstance<RabbitMqAmqpAdapterFactory>(
            serviceProvider,
            providerName,
            connectorFactory,
            rabbitMqOptions,
            cacheOptions,
            hashRingStreamQueueMapperOptions);

        factory.Initialize();

        return factory;
    }
}
