using System.Text.RegularExpressions;

namespace Orleans.Configuration;

internal sealed class RabbitMqOptionsValidator(RabbitMqOptions options, string name) : IConfigurationValidator
{
    private readonly RabbitMqOptions _options = options;
    private readonly string _name = name;

    public void ValidateConfiguration()
    {
        // Validate the connection opinion based on whether a connection string or endpoints are used.
        if (_options.UseConnectionString)
        {
            if (string.IsNullOrWhiteSpace(_options.ConnectionString))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.ConnectionString)} property of {_name} must be set.");

            if (!IsValidRabbitMqConnectionString(_options.ConnectionString))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.ConnectionString)} property of {_name} is not a valid RabbitMQ connection string.");
        }
        else
        {
            if (_options.Endpoints is null || _options.Endpoints.Count == 0)
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.Endpoints)} property of {_name} must be set with at least one endpoint.");

            if (_options.Endpoints.Any(endpoint => string.IsNullOrWhiteSpace(endpoint)))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.Endpoints)} property of {_name} contains one or more empty endpoint strings.");

            if(string.IsNullOrWhiteSpace(_options.VirtualHost))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.VirtualHost)} property of {_name} must be set.");

            if (string.IsNullOrWhiteSpace(_options.UserName))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.UserName)} property of {_name} must be set.");

            if (string.IsNullOrWhiteSpace(_options.Password))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.Password)} property of {_name} must be set.");
        }

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
