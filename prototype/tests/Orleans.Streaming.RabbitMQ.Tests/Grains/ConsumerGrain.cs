using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Streams.Core;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Tests.Grains;

[ImplicitStreamSubscription(TestConstants.MessageStreamNamespace)]
public sealed class ConsumerGrain : Grain, IConsumerGrain, IAsyncObserver<string>, IStreamSubscriptionObserver
{
    private readonly List<string> _receivedMessages = new();
    private readonly ILogger _logger;

    public ConsumerGrain(ILogger<ConsumerGrain> logger)
    {
        _logger = logger;
    }

    public Task<List<string>> GetReceivedMessages()
    {
        return Task.FromResult(_receivedMessages);
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
}