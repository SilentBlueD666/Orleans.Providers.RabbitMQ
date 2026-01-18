using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streaming.RabbitMQ.Config;

namespace Orleans.Hosting;

public static class ClientBuilderExtensions
{
    /// <summary>
    /// Add RabbitMQ AMQP stream provider specifying a provider name and only a connection string; all other settings use defaults.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="connectionString">The RabbitMQ connection string.</param>
    /// <returns>The current instance of <see cref="IClientBuilder"/>.</returns>
    public static IClientBuilder AddRabbitMq(this IClientBuilder builder, string name, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        return builder.AddRabbitMq(name, ob => ob.Configure(options => options.ConnectionString = connectionString));
    }

    /// <summary>
    /// Configure the client to use RabbitMQ AMQP as a persistent streams.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">The action used to configure the RabbitMQ stream provider.</param>
    /// <returns>The current instance of <see cref="IClientBuilder"/>.</returns>
    public static IClientBuilder AddRabbitMq(this IClientBuilder builder, string name, Action<IClusterClientRabbitMqStreamConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var configurator = new ClusterClientRabbitMqStreamConfigurator(
            name: name,
            clientBuilder: builder);

        configure?.Invoke(configurator);

        return builder;
    }

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
