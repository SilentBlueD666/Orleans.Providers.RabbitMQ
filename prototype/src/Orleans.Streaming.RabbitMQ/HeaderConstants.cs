namespace Orleans.Streaming.RabbitMQ;

/// <summary>
/// Header constants for RabbitMQ messages used in Orleans streaming provider.
/// </summary>
public static class HeaderConstants
{
    /// <summary>
    /// Orleans Stream Id header key.
    /// </summary>
    public const string StreamId = "os-stream-id";

    /// <summary>
    /// Orleans Stream Namespace header key.
    /// </summary>
    public const string StreamNamespace = "os-stream-namespace";

    /// <summary>
    /// Orleans Stream Provider Name header key.
    /// </summary>
    public const string StreamProviderName = "os-provider-name";

    /// <summary>
    /// Name of the Queue originally sent via header key.
    /// </summary>
    public const string OriginalQueue = "os-original-queue";
}