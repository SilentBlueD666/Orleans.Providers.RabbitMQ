using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Tests.Grains;

public interface IConsumerGrain : IGrainWithGuidKey
{
    Task<List<string>> GetReceivedMessages();
}