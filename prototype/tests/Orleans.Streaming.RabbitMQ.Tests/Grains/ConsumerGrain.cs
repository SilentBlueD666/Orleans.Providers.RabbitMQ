using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Streams.Core;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Tests.Grains;

[ImplicitStreamSubscription(TestConstants.MessageStreamNamespace)]
public class ConsumerGrain : Grain, IConsumerGrain, IAsyncObserver<string>, IStreamSubscriptionObserver
{
    private readonly List<string> _receivedMessages = new();
    //private StreamSubscriptionHandle<string>? _subscription;
    private readonly ILogger _logger;

    public ConsumerGrain(ILogger<ConsumerGrain> logger)
    {
        _logger = logger;
    }

    //public async Task StartConsuming(Guid streamGuid)
    //{
    //    var streamProvider = this.GetStreamProvider(TestConstants.StreamProvider);
    //    var streamId = StreamId.Create(TestConstants.MessageStreamNamespace, streamGuid);
    //    var stream = streamProvider.GetStream<string>(streamId);
        
    //    _subscription = await stream.SubscribeAsync(OnNextMessage);
    //}

    //private Task OnNextMessage(string message, StreamSequenceToken token)
    //{
    //    _logger.LogInformation("Received message: {Message}", message);
    //    _receivedMessages.Add(message);
    //    return Task.CompletedTask;
    //}

    public Task<List<string>> GetReceivedMessages()
    {
        return Task.FromResult(_receivedMessages);
    }

    public Task StartConsuming(Guid streamGuid)
    {
        throw new NotImplementedException();
    }

    public Task OnNextAsync(string item, StreamSequenceToken? token = null)
    {
        _logger.LogInformation("Received message: {Message}", item);
        _receivedMessages.Add(item);

        return Task.CompletedTask;
    }

    public Task OnErrorAsync(Exception ex)
    {
        _logger.LogWarning(ex, "OnErrorAsync: {Exception}", ex);
        return Task.CompletedTask;
    }

    public Task OnCompletedAsync()
    {
        _logger.LogInformation("OnCompletedAsync");
        return Task.CompletedTask;
    }

    public async Task OnSubscribed(IStreamSubscriptionHandleFactory handleFactory)
    {
        var handle = handleFactory.Create<string>();
        await handle.ResumeAsync(this);
    }

    public override Task OnActivateAsync(CancellationToken token)
    {
        _logger.LogInformation("OnActivateAsync Consumer {PrimaryKey}", this.GetPrimaryKey());
        return Task.CompletedTask;
    }

    //public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    //{
    //    if (_subscription is not null)
    //        await _subscription.UnsubscribeAsync();

    //    await base.OnDeactivateAsync(reason, cancellationToken);
    //}
}