using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Config;

public static class DefaultOptionConstants
{
    public const string ConnectionString = "amqp://guest:guest@localhost:5672/";
    public const string AmqpStreamProviderName = "rabbitmq-amqp-stream-provider";
    public const string QueueNamePrefix = "orleans-queue";
    public const string ExchangeName = "orleans-stream";
    public const string ExchangeType = "direct";
    public const string HostName = "localhost";
    public const int Port = 5672;
    public const int QueueCount = 8;
    public const int MaxConsumerMessages = 5_000;
    public const int PrefetchCount = 1;
    public const string VirtualHost = "/";
    public const string UserName = "guest";
    public const string Password = "guest";
    public const bool Durable = true;
    public const bool AutoDelete = false;

    public static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(30);
}
