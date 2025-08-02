using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Hosting;

public static class ClientBuilderExtensions
{
    public static IClientBuilder AddRabbitMq(
        this IClientBuilder builder,
        string name,
        Action<ClusterClientRabbitMqStreamConfigurator> configure)
    {
        var configurator = new ClusterClientRabbitMqStreamConfigurator(
            name: name,
            clientBuilder: builder);

        configure?.Invoke(configurator);
        
        return builder;
    }

    public static IClientBuilder AddRabbitMq(
        this IClientBuilder builder,
        string name,
        Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
    {
        builder.AddRabbitMq(name, configurator => configurator.ConfigureRabbitMq(configureOptions));
        return builder;
    }
}
