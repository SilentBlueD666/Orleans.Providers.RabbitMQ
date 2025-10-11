using Orleans.Streams;
using RabbitMQ.Client;
using DefaultOptionConstants = Orleans.Streaming.RabbitMQ.Config.DefaultOptionConstants;

namespace Orleans.Configuration;

/// <summary>
/// Represents the configuration options for the RabbitMQ stream provider.
/// </summary>
public sealed class RabbitMqOptions
{
    /// <summary>
    /// The configuration section name for RabbitMQ stream provider options.
    /// </summary>
    public const string SectionName = "Orleans:Streaming:RabbitMQ";

    /// <summary>
    /// The connection string for RabbitMQ.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// A prefix for client-provided name for the RabbitMQ connection.
    /// </summary>
    public string? ConnectionNamePrefix { get; set; }

    /// <summary>
    /// Used to specify endpoints for RabbitMQ connections, for example, for clustering and high availability.
    /// </summary>
    public ICollection<string> Endpoints { get; set; } = [];

    /// <summary>
    /// RabbitMQ virtual host to use for the connection.
    /// </summary>
    public string VirtualHost { get; set; } = DefaultOptionConstants.VirtualHost;

    /// <summary>
    /// RabbitMQ user name to use for the connection.
    /// </summary>
    public string UserName { get; set; } = DefaultOptionConstants.UserName;

    /// <summary>
    /// RabbitMQ password to use for the connection.
    /// </summary>
    public string Password { get; set; } = DefaultOptionConstants.Password;

    /// <summary>
    /// Gets or sets the number of queues to be used in the system.
    /// </summary>
    public int NumberOfQueues { get; set; } = DefaultOptionConstants.QueueCount;

    /// <summary>
    /// Gets or sets the list of queue names to use for the stream provider.
    /// </summary>
    public List<string> QueueNames { get; set; } = [];

    /// <summary>
    /// Gets or sets the prefix used for naming queues.
    /// </summary>
    public string QueueNamePrefix { get; set; } = DefaultOptionConstants.QueueNamePrefix;

    /// <summary>
    /// Whether to declare the queue at start-up or when the first producer sends, consumer subscribes or not at all.
    /// </summary>
    public QueueDeclarationMode QueueDeclaration { get; set; } = QueueDeclarationMode.AtStartup;

    /// <summary>
    /// Defines the type of the queue to be used, such as classic or quorum, or let RabbitMQ use the default configuration.
    /// </summary>
    public QueueType QueueType { get; set; } = QueueType.Default;

    /// <summary>
    /// Gets or sets the number of messages that the consumer can pre-fetch from the queue.
    /// </summary>
    public int PrefetchCount { get; set; } = DefaultOptionConstants.PrefetchCount;

    /// <summary>
    /// Gets or sets a value indicating whether the queue is durable.
    /// </summary>
    public bool QueueDurable { get; set; } = DefaultOptionConstants.Durable;

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
    /// Gets or sets a value indicating whether the exchange is durable.
    /// </summary>
    public bool ExchangeDurable { get; set; } = DefaultOptionConstants.Durable;

    /// <summary>
    /// Maximum number of messages that a receiver can handle at once.
    /// </summary>
    public int MaxConsumerMessages { get; set; } = DefaultOptionConstants.MaxConsumerMessages;

    /// <summary>
    /// Optional; additional queue arguments, e.g. "x-queue-type", used when declaring the queue.
    /// </summary>
    /// <remarks>
    /// <see cref="DeclareQueue"/> must be set to true for these arguments to take effect.
    /// </remarks>
    public IDictionary<string, object?>? QueueArguments = null;

    /// <summary>
    /// Defines whether the stream provider should send messages as a batch or each event individually.
    /// </summary>
    public bool SendAsBatch { get; set; } = false;

    /// <summary>
    /// Gets or sets the mode of the stream provider, indicating the direction of data flow.
    /// </summary>
    /// <remarks>The mode determines whether the stream provider allows reading, writing, or both. Ensure that
    /// the mode is set appropriately before performing any operations to avoid unexpected behaviour.</remarks>
    public StreamProviderDirection Mode { get; set; } = StreamProviderDirection.ReadWrite;

    /// <summary>
    /// Defines whether a stream subscription should be faulted when an error occurs. Default is <see langword="true"/>.
    /// </summary>
    public bool ShouldFaultSubsriptionOnError { get; set; } = true;

    /// <summary>
    /// Defines whether the stream provider should use a connection string or multiple endpoints for RabbitMQ clustering.
    /// </summary>
    internal bool UseConnectionString => !string.IsNullOrWhiteSpace(ConnectionString) && Endpoints.Count == 0;

    /// <summary>
    /// Configures the RabbitMQ connection using a connection string.
    /// </summary>
    /// <param name="connectionString">A valid formatted RabbitMQ connection string, e.g. amqp://guest:guest@localhost:5672/</param>
    public void ConfigureRabbitMqConnection(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        ConnectionString = connectionString;
    }

    /// <summary>
    /// Configures the RabbitMQ connection using individual parameters, that creates a connection string.
    /// </summary>
    /// <param name="hostname">The host name of the RabbitMQ server.</param>
    /// <param name="virtualHost">The virtual host to use for the connection.</param>
    /// <param name="userName">The user name for the RabbitMQ connection.</param>
    /// <param name="password">The password for the RabbitMQ connection.</param>
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

    /// <summary>
    /// Configures the RabbitMQ connection using a collection of endpoints for RabbitMQ clustering.
    /// </summary>
    /// <param name="endpoints">A collection of RabbitMQ endpoints, e.g. ["host1:5672", "host2:5672", "host3"]</param>
    /// <param name="virtualHost">The virtual host to use for the connection.</param>
    /// <param name="userName">The user name for the RabbitMQ connection.</param>
    /// <param name="password">The password for the RabbitMQ connection.</param>
    public void ConfigureRabbitMqConnection(ICollection<string> endpoints, string virtualHost, string userName, string password)
    {
        if (Endpoints is null || Endpoints.Count == 0)
            throw new ArgumentNullException(nameof(Endpoints));

        if (string.IsNullOrWhiteSpace(virtualHost))
            throw new ArgumentNullException(nameof(virtualHost));

        if (string.IsNullOrWhiteSpace(userName))
            throw new ArgumentNullException(nameof(userName));

        if (string.IsNullOrWhiteSpace(password))
            throw new ArgumentNullException(nameof(password));

        Endpoints = endpoints;
        VirtualHost = virtualHost;
        UserName = userName;
        Password = password;
        QueueType = QueueType.Quorum; // Quorum queues are required for clustered setups.

        ConnectionString = null; // Clear any existing connection string
    }
}
