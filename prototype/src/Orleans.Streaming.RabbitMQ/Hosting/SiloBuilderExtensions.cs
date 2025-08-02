using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streaming.RabbitMQ;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Hosting;

public static class SiloBuilderExtensions
{
    private static readonly HashSet<string> _registeredStreamProviders = new HashSet<string>();

    /// <summary>
    /// Configure the silo to use RabbitMQ AMQP as a persistent streams.
    /// </summary>
    /// <param name="builder"></param>
    /// <param name="name"></param>
    /// <param name="configure"></param>
    /// <returns></returns>
    public static ISiloBuilder AddRabbitMq(this ISiloBuilder builder, string name, Action<ISiloRabbitMqStreamConfigurator> configure)
    {
        if (!_registeredStreamProviders.Add(name))
            throw new ArgumentException($"A stream provider with the name '{name}' is already registered.", nameof(name));

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
    /// <param name="builder"></param>
    /// <param name="name"></param>
    /// <param name="configureOptions"></param>
    /// <returns></returns>
    public static ISiloBuilder AddRabbitMq(this ISiloBuilder builder, string name, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
    {
        builder.AddRabbitMq(name, configurator => configurator.ConfigureRabbitMq(configureOptions));

        return builder;
    }
}
