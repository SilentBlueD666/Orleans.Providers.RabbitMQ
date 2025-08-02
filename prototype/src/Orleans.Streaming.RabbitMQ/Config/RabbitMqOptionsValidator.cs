using System.Text.RegularExpressions;

namespace Orleans.Configuration;

public sealed class RabbitMqOptionsValidator(RabbitMqOptions options, string name) : IConfigurationValidator
{
    private readonly RabbitMqOptions _options = options;
    private readonly string _name = name;

    public void ValidateConfiguration()
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.ConnectionString)} property of {_name} must be set.");

        if (!IsValidRabbitMqConnectionString(_options.ConnectionString))
            throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.ConnectionString)} property of {_name} is not a valid RabbitMQ connection string.");

        //if (_options.ExchangeName is { Length : 0})
        //    throw new OrleansConfigurationException($"The {nameof(RabbitMqStreamOptions.ExchangeName)} property of {_name} must be set.");

        //TODO: Check queue names and prefix don't create a length that exceeds RabbitMQ limits.

        if (_options.MaxConsumerMessages < 0)
            throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.MaxConsumerMessages)} property of {_name} must be zero or greater.");
    }

    private bool IsValidRabbitMqConnectionString(string connectionString)
    {
        var regex = new Regex(@"^amqp:\/\/(?<username>[^:]+):(?<password>[^@]+)@(?<hostname>[^\/]+)\/(?<vhost>.*)$");
        return regex.IsMatch(connectionString);
    }

    public static IConfigurationValidator Create(IServiceProvider serviceProvider, string name)
    {
        var options = serviceProvider.GetOptionsByName<RabbitMqOptions>(name);
        return new RabbitMqOptionsValidator(options, name);
    }
}
