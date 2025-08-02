using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Streaming.RabbitMQ.Adapters;
using Orleans.Streaming.RabbitMQ.Adapters.Amqp;
using Orleans.Streaming.RabbitMQ.Tests.Fixtures;
using Orleans.Streaming.RabbitMQ.Tests.Grains;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.RabbitMQ.Tests;

public class RabbitMqStreamingTests : IClassFixture<RabbitMqTestClusterFixture>
{
    private readonly RabbitMqTestClusterFixture _fixture;
    private readonly ILogger<RabbitMqStreamingTests> _logger;

    public RabbitMqStreamingTests(RabbitMqTestClusterFixture fixture, ITestOutputHelper outputHelper)
    {
        _fixture = fixture;
        _logger = fixture
            .LoggerFactory
            .AddXunit(outputHelper)
            .CreateLogger<RabbitMqStreamingTests>();
    }

    [Fact]
    public async Task ProducerSendsAndConsumerReceives()
    {
        // Arrange
        var streamGuid = Guid.NewGuid();
        var producer = _fixture.GrainFactory.GetGrain<IProducerGrain>(streamGuid);
        
        
        // Act
        //await consumer.StartConsuming(streamGuid);
        
        var testMessage = "Hello RabbitMQ Streaming!";
        await producer.SendMessage(testMessage);
        
        // Poll for message with timeout
        var timeout = TimeSpan.FromSeconds(30);
        var interval = TimeSpan.FromSeconds(1);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        List<string> receivedMessages = new();
        bool messageReceived = false;

        var consumer = _fixture.GrainFactory.GetGrain<IConsumerGrain>(streamGuid);
        while (stopwatch.Elapsed < timeout && !messageReceived)
        {
            receivedMessages = await consumer.GetReceivedMessages();
            if (receivedMessages.Contains(testMessage))
            {
                messageReceived = true;
                break;
            }
            
            await Task.Delay(interval);
        }
        
        // Assert
        Assert.Contains(testMessage, receivedMessages);
    }
    
    [Fact]
    public async Task MultipleMessagesDeliveredInOrder()
    {
        // Arrange
        var streamGuid = Guid.NewGuid();
        var producer = _fixture.GrainFactory.GetGrain<IProducerGrain>(streamGuid);
        var consumer = _fixture.GrainFactory.GetGrain<IConsumerGrain>(streamGuid);
        
        // Act
        await consumer.StartConsuming(streamGuid);
        
        var messages = new List<string> { "First", "Second", "Third" };
        foreach (var message in messages)
        {
            await producer.SendMessage(message);
        }
        
        // Poll for messages with timeout
        var timeout = TimeSpan.FromSeconds(60);
        var interval = TimeSpan.FromSeconds(1);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        List<string> receivedMessages = new();
        bool allMessagesReceived = false;
        
        while (stopwatch.Elapsed < timeout && !allMessagesReceived)
        {
            receivedMessages = await consumer.GetReceivedMessages();
            if (receivedMessages.Count == messages.Count)
            {
                bool allFound = true;
                // Verify all messages are present and in order
                for (int i = 0; i < messages.Count; i++)
                {
                    if (i >= receivedMessages.Count || receivedMessages[i] != messages[i])
                    {
                        allFound = false;
                        break;
                    }
                }
                
                allMessagesReceived = allFound;
                if (allMessagesReceived)
                    break;
            }
            
            await Task.Delay(interval);
        }
        
        // Assert
        Assert.Equal(messages.Count, receivedMessages.Count);
        
        // Check message order is preserved
        for (int i = 0; i < messages.Count; i++)
        {
            Assert.Equal(messages[i], receivedMessages[i]);
        }
    }

    [Fact]
    public async Task CanPublishAndReadFromRabbitMq()
    {
        // Arrange
        var streamGuid = Guid.NewGuid();
        var producer = _fixture.GrainFactory.GetGrain<IProducerGrain>(streamGuid);

        // Get service provider to directly access adapter
        var serviceProvider = _fixture.HostedCluster.ServiceProvider;
        var adapterFactory = RabbitMqAmqpAdapterFactory.Create(serviceProvider, "RabbitMQ");

        var adapter = await adapterFactory.CreateAdapter();
        
        // Get a receiver for the queue that should have our message
        var mapper = adapterFactory.GetStreamQueueMapper();
        var streamId = StreamId.Create("MessageStream", streamGuid);
        var queueId = mapper.GetQueueForStream(streamId);
        var receiver = adapter.CreateReceiver(queueId);
        
        // Get messages directly
        await receiver.Initialize(TimeSpan.FromSeconds(5));

        // Act
        var testMessage = "Direct Test Message";
        await producer.SendMessage(testMessage);

        await Task.Delay(2000); // Wait for message to be processed
        var messages = await receiver.GetQueueMessagesAsync(10);
        
        // Assert
        Assert.NotEmpty(messages);
        var batchContainer = messages.First() as RabbitMqBatchContainer;
        Assert.NotNull(batchContainer);
        
        // Check stream ID
        Assert.Equal(streamId, batchContainer.StreamId);
        
        // Get the actual message content
        var events = batchContainer.GetEvents<string>().ToList();
        Assert.NotEmpty(events);
        Assert.Equal(testMessage, events.First().Item1);
    }
}