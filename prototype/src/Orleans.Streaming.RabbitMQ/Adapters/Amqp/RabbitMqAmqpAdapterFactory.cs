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

internal sealed class RabbitMqAmqpAdapterFactory : IQueueAdapterFactory
{
    private readonly IPendingDeliveryTracker _pendingDeliveryTracker;
    private readonly Func<QueueId, Task<IStreamFailureHandler>>? _streamFailureHandlerFactory;
    private readonly TimeProvider _timeProvider;
    private readonly IRabbitMqDataAdapter _dataAdapter;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IRabbitMqQueueProvider _queueProvider;
    private readonly RabbitMqOptions _options;
    private readonly SimpleQueueAdapterCache _adapterCache;
    private readonly string _providerName;
    private readonly IRabbitMqConnectorFactory _connectorFactory;

    private RabbitMqAmqpAdapterFactory(
        string providerName,
        IRabbitMqConnectorFactory connectorFactory,
        IRabbitMqQueueProvider rabbitMqQueueProvider,
        RabbitMqOptions options,
        SimpleQueueCacheOptions cacheOptions,
        IRabbitMqDataAdapter dataAdapter,
        IPendingDeliveryTracker pendingDeliveryTracker,
        Func<QueueId, Task<IStreamFailureHandler>>? streamFailureHandlerFactory,
        TimeProvider timeProvider,
        ILoggerFactory loggerFactory)
    {         
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));
        _connectorFactory = connectorFactory ?? throw new ArgumentNullException(nameof(connectorFactory));
        _queueProvider = rabbitMqQueueProvider ?? throw new ArgumentNullException(nameof(rabbitMqQueueProvider));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _pendingDeliveryTracker = pendingDeliveryTracker ?? throw new ArgumentNullException(nameof(pendingDeliveryTracker));
        _streamFailureHandlerFactory = streamFailureHandlerFactory;
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _dataAdapter = dataAdapter ?? throw new ArgumentNullException(nameof(dataAdapter));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));

        _adapterCache = new(cacheOptions, providerName, loggerFactory);
    }

    public Func<QueueId, Task<IStreamFailureHandler>>? StreamFailureHandlerFactory { get; private set; }

    public void Initialize()
    {
        StreamFailureHandlerFactory = _streamFailureHandlerFactory
            ?? (_ => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler()));
    }

    public async Task<IQueueAdapter> CreateAdapter()
    {
        var adapter = new RabbitMqAmqpAdapter(
            providerName: _providerName,
            connectorFactory: _connectorFactory,
            dataAdapter: _dataAdapter,
            queueProvider: _queueProvider,
            pendingDeliveryTracker: _pendingDeliveryTracker,
            options: _options,
            timeProvider: _timeProvider,
            loggerFactory: _loggerFactory);

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
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

        var rabbitMqOptions = serviceProvider.GetOptionsByName<RabbitMqOptions>(providerName);

        var cacheOptions = serviceProvider.GetOptionsByName<SimpleQueueCacheOptions>(providerName);

        var connectorFactory = serviceProvider.GetRequiredKeyedService<IRabbitMqConnectorFactory>(providerName);

        var pendingDeliveryTracker = serviceProvider.GetRequiredKeyedService<IPendingDeliveryTracker>(providerName);

        var streamFailureHandlerFactory = serviceProvider.GetKeyedService<Func<QueueId, Task<IStreamFailureHandler>>>(providerName);      

        var rabbitMqQueueProvider = serviceProvider.GetRequiredKeyedService<IRabbitMqQueueProvider>(providerName);

        var dataAdapter = serviceProvider.GetKeyedService<IRabbitMqDataAdapter>(providerName)
            ?? serviceProvider.GetRequiredService<IRabbitMqDataAdapter>();

        var adapterFactory = new RabbitMqAmqpAdapterFactory(
            providerName: providerName,
            connectorFactory: connectorFactory,
            rabbitMqQueueProvider: rabbitMqQueueProvider,
            options: rabbitMqOptions,
            cacheOptions: cacheOptions,
            dataAdapter: dataAdapter,
            pendingDeliveryTracker: pendingDeliveryTracker,
            streamFailureHandlerFactory: streamFailureHandlerFactory,
            timeProvider: serviceProvider.GetRequiredService<TimeProvider>(),
            loggerFactory: loggerFactory);

        adapterFactory.Initialize();

        return adapterFactory;
    }
}
