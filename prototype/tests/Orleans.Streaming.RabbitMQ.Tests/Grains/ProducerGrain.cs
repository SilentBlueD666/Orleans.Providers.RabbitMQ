using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Streaming.RabbitMQ.Tests.Grains;

public class ProducerGrain : Grain, IProducerGrain
{
    private IAsyncStream<string>? _stream;
    private readonly ILogger _logger;

    public ProducerGrain(ILogger<ProducerGrain> logger)
    {
        _logger = logger;
    }

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var streamId = StreamId.Create(TestConstants.MessageStreamNamespace, this.GetPrimaryKey());
        var streamProvider = this.GetStreamProvider(TestConstants.StreamProvider);
        
        _stream = streamProvider.GetStream<string>(streamId);
        await base.OnActivateAsync(cancellationToken);
    }

    public async Task SendMessage(string message)
    {
        _logger.LogInformation("Sending message: {Message}", message);
        if (_stream is null)
        {
            _logger.LogError("Stream is not initialized.");
            throw new InvalidOperationException("Stream is not initialized.");
        }

        await _stream.OnNextAsync(message);
    }
}