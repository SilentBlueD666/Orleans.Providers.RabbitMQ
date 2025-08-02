using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Tests.Grains;

public interface IProducerGrain : IGrainWithGuidKey
{
    Task SendMessage(string message);
}