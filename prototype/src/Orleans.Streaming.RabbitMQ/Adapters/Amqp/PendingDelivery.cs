namespace Orleans.Streaming.RabbitMQ.Adapters.Amqp;

internal readonly struct PendingDelivery
{
    public ulong DeliveryTag { get; }
    public DateTime Timestamp { get; }

    public PendingDelivery(ulong deliveryTag)
    {
        DeliveryTag = deliveryTag;
        Timestamp = DateTime.UtcNow;
    }
}