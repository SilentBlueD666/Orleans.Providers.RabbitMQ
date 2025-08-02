using RabbitMQ.Client;
using DefaultOptionConstants = Orleans.Streaming.RabbitMQ.Config.DefaultOptionConstants;

namespace Orleans.Configuration;

/// <summary>
/// Represents the configuration options for the RabbitMQ stream provider.
/// </summary>
public sealed class RabbitMqOptions
{
    /// <summary>
    /// The connection string for RabbitMQ.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// The client-provided name for the RabbitMQ connection.
    /// </summary>
    public string? ConnectionName { get; set; }


    public List<string> HostNames { get; set; } = [];

    public string VirtualHost { get; set; } = DefaultOptionConstants.VirtualHost;

    public int Port { get; set; } = DefaultOptionConstants.Port;
    public string UserName { get; set; } = DefaultOptionConstants.UserName;
    public string Password { get; set; } = DefaultOptionConstants.Password;


    /// <summary>
    /// Gets or sets the list of queue names to use for the stream provider.
    /// </summary>
    public List<string> QueueNames { get; set; } = [];

    /// <summary>
    /// Gets or sets the prefix used for naming queues.
    /// </summary>
    public string QueueNamePrefix { get; set; } = DefaultOptionConstants.QueueNamePrefix;

    /// <summary>
    /// Whether to declare the queue when the stream provider is initialized.
    /// </summary>
    public bool DeclareQueue { get; set; } = true;
    
    /// <summary>
    /// Gets or sets the number of messages that the consumer can pre-fetch from the queue.
    /// </summary>
    public int PrefetchCount { get; set; } = DefaultOptionConstants.PrefetchCount;
    
    /// <summary>
    /// Gets or sets a value indicating whether the operation is durable.
    /// </summary>
    public bool Durable { get; set; } = DefaultOptionConstants.Durable;

    /// <summary>
    /// If true, the queue will be automatically deleted when the last consumer unsubscribes.
    /// </summary>
    public bool AutoDelete { get; set; } = DefaultOptionConstants.AutoDelete;

    /// <summary>
    /// The name of the exchange to use for publishing messages.
    /// </summary>
    public string ExchangeName { get; set; } = DefaultOptionConstants.ExchangeName;

    /// <summary>
    /// The type of the exchange to use for publishing messages.
    /// </summary>
    public string ExchangeType { get; set; } = DefaultOptionConstants.ExchangeType;

    /// <summary>
    /// Maximum number of messages that the receiver can handle at once.
    /// </summary>
    public int MaxConsumerMessages { get; set; } = 5_000;

    /// <summary>
    /// Optional; additional queue arguments, e.g. "x-queue-type", used when declaring the queue.
    /// </summary>
    /// <remarks>
    /// <see cref="DeclareQueue"/> must be set to true for these arguments to be used.
    /// </remarks>
    public IDictionary<string, object?>? QueueArguments = null;

    /// <summary>
    /// Defines whether the stream provider should send messages as a batch or each event singularly.
    /// </summary>
    public bool SendAsBatch { get; set; } = false;

    internal bool UseConnectionString => !string.IsNullOrWhiteSpace(ConnectionString);

    public void ConfigureRabbitMqConnection(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        ConnectionString = connectionString;
    }

    public void ConfigureRabbitMqConnection(string hostname, string virtualHost, string userName, string password)
    {
        if (string.IsNullOrWhiteSpace(hostname))
            throw new ArgumentNullException(nameof(hostname));

        if (string.IsNullOrWhiteSpace(virtualHost))
            throw new ArgumentNullException(nameof(virtualHost));

        if (string.IsNullOrWhiteSpace(userName))
            throw new ArgumentNullException(nameof(userName));

        if (string.IsNullOrWhiteSpace(password))
            throw new ArgumentNullException(nameof(password));

        ConnectionString = $"amqp://{userName}:{password}@{hostname}/{virtualHost.TrimStart('/')}";
    }


}
