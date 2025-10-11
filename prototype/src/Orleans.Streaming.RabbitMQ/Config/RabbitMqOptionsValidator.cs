using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Configuration;

internal sealed class RabbitMqOptionsValidator(RabbitMqOptions options, string name) : IConfigurationValidator
{
    private const int RABBITMQ_MAX_QUEUE_NAME_LENGTH = 255;
    private static readonly char[] _invalidChars = [' ', '*', '#', ':', '\\', '/', '{', '}', '[', ']', ',', '"', '\'', '|', '<', '>', '=', '!', '@'];
    private static readonly Regex _rabbitMqConnectionStringRegex = new(@"^amqp:\/\/(?<username>[^:]+):(?<password>[^@]+)@(?<hostname>[^\/]+)\/(?<vhost>.*)$");

    private readonly RabbitMqOptions _options = options;
    private readonly string _name = name;

    public void ValidateConfiguration()
    {
        // Validate the connection opinion based on whether a connection string or endpoints are used.
        if (_options.UseConnectionString)
        {
            var connectionString = _options.ConnectionString;
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.ConnectionString)} property of {_name} must be set.");

            if (!IsValidRabbitMqConnectionString(connectionString))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.ConnectionString)} property of {_name} is not a valid RabbitMQ connection string.");
        }
        else
        {
            if (_options.Endpoints is null or { Count: 0 })
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.Endpoints)} property of {_name} must be set with at least one endpoint.");

            if (_options.Endpoints.Any(endpoint => string.IsNullOrWhiteSpace(endpoint)))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.Endpoints)} property of {_name} contains one or more empty endpoint strings.");

            if (string.IsNullOrWhiteSpace(_options.VirtualHost))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.VirtualHost)} property of {_name} must be set.");

            if (string.IsNullOrWhiteSpace(_options.UserName))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.UserName)} property of {_name} must be set.");

            if (string.IsNullOrWhiteSpace(_options.Password))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.Password)} property of {_name} must be set.");

            if (_options.QueueType == QueueType.Classic)
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.QueueType)} property of {_name} cannot be set to {QueueType.Classic} when using endpoints. Use {QueueType.Quorum} or {QueueType.Default}.");
        }

        var queueNamePrefix = _options.QueueNamePrefix.AsSpan();
        if (queueNamePrefix.IndexOfAny(_invalidChars) != -1)
            throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.QueueNamePrefix)} property of {_name} contains one or more invalid characters: {string.Join(' ', _invalidChars)}");

        var queueNamePrefixLength = Encoding.UTF8.GetByteCount(queueNamePrefix);

        foreach (var queueName in _options.QueueNames)
        {
            if (string.IsNullOrWhiteSpace(queueName))
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.QueueNames)} property of {_name} contains one or more empty queue names.");

            if (TotalQueueNameLength(queueNamePrefixLength, queueName) > RABBITMQ_MAX_QUEUE_NAME_LENGTH)
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.QueueNames)}.{queueNamePrefix}{queueName} property of {_name} exceeds the maximum length of {RABBITMQ_MAX_QUEUE_NAME_LENGTH} characters including prefix.");

            if (queueName.IndexOfAny(_invalidChars) != -1)
                throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.QueueNames)}.{queueName} property of {_name} contains one or more invalid characters: {string.Join(' ', _invalidChars)}");
        }

        if (_options.MaxConsumerMessages < 0)
            throw new OrleansConfigurationException($"The {nameof(RabbitMqOptions.MaxConsumerMessages)} property of {_name} must be zero or greater.");

        static int TotalQueueNameLength(int totalPrefixLength, ReadOnlySpan<char> queueName)
            => totalPrefixLength + Encoding.UTF8.GetByteCount(queueName);

        static bool IsValidRabbitMqConnectionString(ReadOnlySpan<char> connectionString)
            => _rabbitMqConnectionStringRegex.IsMatch(connectionString);
    }

    public static IConfigurationValidator Create(IServiceProvider serviceProvider, string name)
    {
        var options = serviceProvider.GetOptionsByName<RabbitMqOptions>(name);
        return new RabbitMqOptionsValidator(options, name);
    }
}
