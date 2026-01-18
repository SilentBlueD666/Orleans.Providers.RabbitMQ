using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streaming.RabbitMQ;
using Orleans.Streaming.RabbitMQ.Config;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Hosting;

public static class SiloBuilderExtensions
{
    /// <summary>
    /// Add RabbitMQ AMQP stream provider specifying a provider name and only a connection string; all other settings use defaults.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="connectionString">The RabbitMQ connection string.</param>
    /// <returns>The current instance of <see cref="ISiloBuilder"/>.</returns>
    public static ISiloBuilder AddRabbitMq(this ISiloBuilder builder, string name, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        return builder.AddRabbitMq(name, ob => ob.Configure(options => options.ConnectionString = connectionString));
    }

    /// <summary>
    /// Configure the silo to use RabbitMQ AMQP as a persistent streams.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">The action used to configure the RabbitMQ stream provider.</param>
    /// <returns>The current instance of <see cref="ISiloBuilder"/>.</returns>
    public static ISiloBuilder AddRabbitMq(this ISiloBuilder builder, string name, Action<ISiloRabbitMqStreamConfigurator> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        builder.AddMemoryGrainStorage(name);

        var configurator = new SiloRabbitMqStreamConfigurator(
            name: name,
            configureServicesDelegate: configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate));

        configure?.Invoke(configurator);

        return builder;
    }

    /// <summary>
    /// Configure the silo to use RabbitMQ AMQP as a persistent streams with default settings.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configureOptions">The action used to configure the RabbitMQ options.</param>
    /// <returns>The current instance of <see cref="ISiloBuilder"/>.</returns>
    public static ISiloBuilder AddRabbitMq(this ISiloBuilder builder, string name, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
        => builder.AddRabbitMq(name, configurator => configurator.ConfigureRabbitMq(configureOptions));
}
