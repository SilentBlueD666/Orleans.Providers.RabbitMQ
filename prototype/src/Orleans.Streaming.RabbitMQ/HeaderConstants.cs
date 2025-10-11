namespace Orleans.Streaming.RabbitMQ;

public static class HeaderConstants
{
    public const string StreamId = "orleans-stream-id";
    public const string StreamNamespace = "orleans-stream-namespace";
    public const string StreamProviderName = "orleans-stream-provider-name";

    public const string OriginalQueue = "orleans-stream-original-queue";
}