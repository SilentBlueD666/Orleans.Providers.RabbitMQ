using GrainInterfaces;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streams.Core;

namespace Grains;

// ImplicitStreamSubscription attribute here is to subscribe implicitely to all stream within
// a given namespace: whenever some data is pushed to the streams of namespace Constants.StreamNamespace,
// a grain of type ConsumerGrain with the same guid of the stream will receive the message.
// Even if no activations of the grain currently exist, the runtime will automatically
// create a new one and send the message to it.
[ImplicitStreamSubscription(Constants.StreamNamespace)]
public sealed partial class ConsumerGrain : Grain, IConsumerGrain, IStreamSubscriptionObserver
{
    private readonly ILogger<IConsumerGrain> _logger;
    private readonly LoggerObserver _observer;

    public ConsumerGrain(ILogger<IConsumerGrain> logger)
    {
        _logger = logger;
        _observer = new LoggerObserver(_logger, this.GetPrimaryKey());
    }

    // Called when a subscription is added
    public async Task OnSubscribed(IStreamSubscriptionHandleFactory handleFactory)
    {
        // Plug our LoggerObserver to the stream
        var handle = handleFactory.Create<int>();
        await handle.ResumeAsync(_observer);
    }


    public override Task OnActivateAsync(CancellationToken token)
    {
        _logger.LogInformation("OnActivateAsync");
        return Task.CompletedTask;
    }

    /// <summary>
    /// Class that will log streaming events
    /// </summary>
    private partial class LoggerObserver : IAsyncObserver<int>
    {
        private readonly ILogger<IConsumerGrain> _logger;
        private readonly Guid _grainId;

        public LoggerObserver(ILogger<IConsumerGrain> logger, Guid grainId)
        {
            _logger = logger;
            _grainId = grainId;
        }

        public Task OnCompletedAsync()
        {
            _logger.LogInformation("OnCompletedAsync");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            _logger.LogInformation("OnErrorAsync: {Exception}", ex);
            return Task.CompletedTask;
        }

        public Task OnNextAsync(int item, StreamSequenceToken? token = null)
        {
            //if (item == 10)
            //    throw new Exception("This is a test exception for logging purposes");

            LogOnNext(_grainId, item, token);
            return Task.CompletedTask;
        }

        [LoggerMessage(
            EventId = 1001,
            Level = LogLevel.Debug,
            Message = "Consumer Grain {GrainId} OnNextAsync: item: {Item}, token = {Token}")]
        partial void LogOnNext(Guid grainId, int item, StreamSequenceToken? token = null);
    }
}
