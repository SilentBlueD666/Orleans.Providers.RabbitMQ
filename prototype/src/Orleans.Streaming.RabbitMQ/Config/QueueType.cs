namespace Orleans.Configuration;

/// <summary>
/// The type of RabbitMQ queue to use.
/// </summary>
public enum QueueType
{
    /// <summary>
    /// Let the RabbitMQ server decide the queue type (default).
    /// </summary>
    Default,
    /// <summary>
    /// Use classic RabbitMQ queues.
    /// </summary>
    Classic,
    /// <summary>
    /// Use RabbitMQ Quorum queues.
    /// </summary>
    Quorum
}