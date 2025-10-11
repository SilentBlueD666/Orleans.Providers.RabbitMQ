using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streaming.RabbitMQ;
using Orleans.Streaming.RabbitMQ.Adapters;
using Orleans.Streaming.RabbitMQ.Adapters.Amqp;
using Orleans.Streams;

namespace Orleans.Hosting;

public interface IRabbitMqStreamConfigurator : INamedServiceConfigurator { }

public static class RabbitMqStreamConfiguratorExtensions
{
    public static void ConfigureRabbitMq(this IRabbitMqStreamConfigurator configurator, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
    {
        configurator.Configure(configureOptions);
    }

    public static void UseDataAdapter<TQueueDataAdapter>(this IRabbitMqStreamConfigurator configurator)
        where TQueueDataAdapter : class, IRabbitMqDataAdapter
    {
        configurator.ConfigureComponent<IRabbitMqDataAdapter>((sp, _) => ActivatorUtilities.CreateInstance<TQueueDataAdapter>(sp));
    }

    public static void UseDataAdapter(this IRabbitMqStreamConfigurator configurator, Func<IServiceProvider, string, IRabbitMqDataAdapter> factory)
    {
        configurator.ConfigureComponent(factory);
    }
}

public interface ISiloRabbitMqStreamConfigurator : IRabbitMqStreamConfigurator, ISiloPersistentStreamConfigurator { }

public static class SiloRabbitMqStreamConfiguratorExtensions
{
    public static void ConfigureCacheSize(this ISiloRabbitMqStreamConfigurator configurator, int cacheSize = SimpleQueueCacheOptions.DEFAULT_CACHE_SIZE)
    {
        configurator.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
    }
}

public sealed class SiloRabbitMqStreamConfigurator : SiloPersistentStreamConfigurator, ISiloRabbitMqStreamConfigurator
{
    public SiloRabbitMqStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate)
        : base(name, configureServicesDelegate, RabbitMqAmqpAdapterFactory.Create)
    {
        this.ConfigureComponent(RabbitMqOptionsValidator.Create);
        this.ConfigureComponent(SimpleQueueCacheOptionsValidator.Create);
        this.Configure<RabbitMqOptions>(ob
            => ob.PostConfigure(options
                => options.ExchangeName = string.IsNullOrWhiteSpace(options.ExchangeName)
                    ? name
                    : options.ExchangeName));

        this.ConfigureComponent<IRabbitMqConnectionProvider>((sp, key) =>
        {
            var options = sp.GetOptionsByName<RabbitMqOptions>(key);
            return ActivatorUtilities.CreateInstance<RabbitMqConnectionProvider>(sp, options);
        });

        this.ConfigureComponent<IRabbitMqConnectorFactory>((sp, key) =>
        {
            var connectionProvider = sp.GetRequiredKeyedService<IRabbitMqConnectionProvider>(key);
            var options = sp.GetOptionsByName<RabbitMqOptions>(key);
            return ActivatorUtilities.CreateInstance<RabbitMqConnectorFactory>(sp, connectionProvider, options);
        });

        this.ConfigureComponent<IRabbitMqQueueProvider>((sp, key) =>
        {
            var options = sp.GetOptionsByName<RabbitMqOptions>(key);
            return RabbitMqAmqpQueueProvider.Create(key, options);
        });

        this.ConfigureComponent<Func<QueueId, Task<IStreamFailureHandler>>>((sp, key) =>
        {
            var options = sp.GetOptionsByName<RabbitMqOptions>(key);
            var pendingDeliveryTracker = sp.GetRequiredKeyedService<IPendingDeliveryTracker>(key);
            var queueProvider = sp.GetRequiredKeyedService<IRabbitMqQueueProvider>(key);
            var connectorFactory = sp.GetRequiredKeyedService<IRabbitMqConnectorFactory>(key);

            Func<QueueId, Task<IStreamFailureHandler>> streamFailureHandlerFactory = _ =>
            {
                var instance = ActivatorUtilities.CreateInstance<RabbitMqAmqpStreamFailureHandler>(
                    sp, 
                    pendingDeliveryTracker,
                    connectorFactory,
                    queueProvider,
                    options);

                return Task.FromResult<IStreamFailureHandler>(instance);
            };

            return streamFailureHandlerFactory;
        });

        this.ConfigureDelegate(services => services.TryAddSingleton<IRabbitMqDataAdapter, RabbitMqDataAdapter>());
        this.ConfigureComponent<IPendingDeliveryTracker, PendingDeliveryTracker>();
    }
}

public interface IClusterClientRabbitMqStreamConfigurator : IRabbitMqStreamConfigurator, IClusterClientPersistentStreamConfigurator { }

public class ClusterClientRabbitMqStreamConfigurator : ClusterClientPersistentStreamConfigurator, IClusterClientRabbitMqStreamConfigurator
{
    public ClusterClientRabbitMqStreamConfigurator(string name, IClientBuilder clientBuilder)
        : base(name, clientBuilder, RabbitMqAmqpAdapterFactory.Create)
    {
        this.ConfigureComponent(RabbitMqOptionsValidator.Create);

        this.Configure<RabbitMqOptions>(ob
            => ob.PostConfigure(options
                => options.ExchangeName = string.IsNullOrWhiteSpace(options.ExchangeName)
                    ? name
                    : options.ExchangeName));

        this.ConfigureComponent<IRabbitMqConnectionProvider>((sp, key) =>
        {
            var options = sp.GetOptionsByName<RabbitMqOptions>(key);
            return ActivatorUtilities.CreateInstance<RabbitMqConnectionProvider>(sp, options);
        });

        this.ConfigureComponent<IRabbitMqConnectorFactory>((sp, key) =>
        {
            var connectionProvider = sp.GetRequiredKeyedService<IRabbitMqConnectionProvider>(key);
            var options = sp.GetOptionsByName<RabbitMqOptions>(key);
            return ActivatorUtilities.CreateInstance<RabbitMqConnectorFactory>(sp, connectionProvider, options);
        });

        this.ConfigureComponent<IRabbitMqQueueProvider>((sp, key) =>
        {
            var options = sp.GetOptionsByName<RabbitMqOptions>(key);
            return RabbitMqAmqpQueueProvider.Create(key, options);
        });

        this.ConfigureDelegate(services => services.TryAddSingleton<IRabbitMqDataAdapter, RabbitMqDataAdapter>());
        this.ConfigureComponent<IPendingDeliveryTracker, PendingDeliveryTracker>();
    }
}

file static class NamedServiceConfiguratorExtensions
{
    /// <summary>
    /// Adds a singleton component to a named service, with a specific implementation type.
    /// </summary>
    /// <remarks>This method registers the specified implementation type as a singleton for the named service.
    /// The implementation is created using dependency injection via <see cref="ActivatorUtilities"/>.</remarks>
    /// <typeparam name="TService">The type of the service interface or base class.</typeparam>
    /// <typeparam name="TImplementation">The type of the concrete implementation to be registered. Must derive from <typeparamref name="TService"/>.</typeparam>
    /// <param name="configurator">The configurator used to define the named service registration.</param>
    public static void ConfigureComponent<TService, TImplementation>(this INamedServiceConfigurator configurator)
        where TService : class
        where TImplementation : class, TService
    {
        configurator.ConfigureDelegate(services => services.TryAddKeyedSingleton<TService>(configurator.Name, (sp, _) => ActivatorUtilities.CreateInstance<TImplementation>(sp)));
    }
}