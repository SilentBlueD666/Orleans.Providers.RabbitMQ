using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streaming.RabbitMQ.Config;

namespace Orleans.Hosting;

public static class ClientBuilderExtensions
{
    private static readonly HashSet<string> _registeredStreamProviders = new HashSet<string>();

    /// <summary>
    /// Add RabbitMQ AMQP stream provider with default name and options configured from the <c>RabbitMqOptions.SectionName</c> section of the configuration.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <returns>The current instance of <see cref="IClientBuilder"/>.</returns>
    public static IClientBuilder AddRabbitMq(this IClientBuilder builder)
        => builder.AddRabbitMq(DefaultOptionConstants.AmqpStreamProviderName);

    /// <summary>
    /// Add RabbitMQ AMQP stream provider with options configured from the <c>RabbitMqOptions.SectionName</c> section of the configuration.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <returns>The current instance of <see cref="IClientBuilder"/>.</returns>
    public static IClientBuilder AddRabbitMq(this IClientBuilder builder, string name)
        => builder
            .AddRabbitMq(name, (Action<OptionsBuilder<RabbitMqOptions>>)(options =>
            {
                builder.Configuration.GetSection(RabbitMqOptions.SectionName).Bind(options);
            }));

    /// <summary>
    /// Configure the client to use RabbitMQ AMQP as a persistent streams with default name.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="configure">The action used to configure the RabbitMQ stream provider.</param>
    /// <returns>The current instance of <see cref="IClientBuilder"/>.</returns>
    public static IClientBuilder AddRabbitMq(this IClientBuilder builder, Action<IClusterClientRabbitMqStreamConfigurator> configure)
        => builder.AddRabbitMq(DefaultOptionConstants.AmqpStreamProviderName, configure);

    /// <summary>
    /// Configure the client to use RabbitMQ AMQP as a persistent streams.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">The action used to configure the RabbitMQ stream provider.</param>
    /// <returns>The current instance of <see cref="IClientBuilder"/>.</returns>
    public static IClientBuilder AddRabbitMq(this IClientBuilder builder, string name, Action<IClusterClientRabbitMqStreamConfigurator> configure)
    {
        if (!_registeredStreamProviders.Add(name))
            throw new ArgumentException($"A stream provider with the name '{name}' is already registered.", nameof(name));

        var configurator = new ClusterClientRabbitMqStreamConfigurator(
            name: name,
            clientBuilder: builder);

        configure?.Invoke(configurator);

        return builder;
    }

    /// <summary>
    /// Configure the client to use RabbitMQ AMQP as a persistent streams with default name and settings.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="configureOptions">The action used to configure the RabbitMQ options.</param>
    /// <returns>The current instance of <see cref="IClientBuilder"/>.</returns>
    public static IClientBuilder AddRabbitMq(this IClientBuilder builder, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
        => builder.AddRabbitMq(DefaultOptionConstants.AmqpStreamProviderName, configureOptions);

    /// <summary>
    /// Configure the client to use RabbitMQ AMQP as a persistent streams with default settings.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configureOptions">The action used to configure the RabbitMQ options.</param>
    /// <returns>The current instance of <see cref="IClientBuilder"/>.</returns>
    public static IClientBuilder AddRabbitMq(this IClientBuilder builder, string name, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
       => builder.AddRabbitMq(name, configurator => configurator.ConfigureRabbitMq(configureOptions));
}
