using Common;
using GrainInterfaces;
using Orleans.Configuration;
using Orleans.Streams;

const int maxAttempts = 5;
var attempt = 0;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddKeyedRedisClient("redis");

builder.Logging
    .AddConsole()
    .SetMinimumLevel(LogLevel.Debug)
    .AddFilter("Orleans", LogLevel.Information);

builder.UseOrleansClient(client 
    => client
        .UseConnectionRetryFilter(RetryFilter)
        .AddRabbitMq(Constants.StreamProvider, ob =>
        {
            ob.Configure(options =>
            {
                options.ConnectionString = builder.Configuration.GetConnectionString("rabbitmq")!;
                options.ExchangeName = "orleans-test-exchange";
                options.QueueNamePrefix = "orleans-test-queue";
                options.ConnectionNamePrefix = "Orleans.Streaming.Client.RabbitMQ";
            });
        }));

var app = builder.Build();

app.UseFileServer();

await app.StartAsync();

Console.WriteLine("Client successfully connect to silo host");

var client = app.Services.GetRequiredService<IClusterClient>();

// Use the connected client to ask a grain to start producing events
var key = Guid.NewGuid();
var producer = client.GetGrain<IProducerGrain>("my-producer");
await producer.StartProducing(Constants.StreamNamespace, key);

// Now you should see that a consumer grain was activated on the silo, and is logging when it is receiving event

// Client can also subscribe to streams
var streamId = StreamId.Create(Constants.StreamNamespace, key);
var stream = client
    .GetStreamProvider(Constants.StreamProvider)
    .GetStream<int>(streamId);

var handle = await stream.SubscribeAsync(OnNextAsync);

// Now the client will also log received events

await Task.Delay(TimeSpan.FromSeconds(30));

// Stop producing
await producer.StopProducing();
await handle.UnsubscribeAsync();

// Now we going to try a performance test
Console.WriteLine("Starting performance test...");
var stopwatch = System.Diagnostics.Stopwatch.StartNew();

for (var i = 0; i < 10_000; i++)
{
    await stream.OnNextAsync(i);
}

stopwatch.Stop();

Console.WriteLine($"Performance test completed in {stopwatch.ElapsedMilliseconds} ms for 10,000 messages.");

Console.WriteLine("Press any key to exit...");
Console.ReadKey();

static Task OnNextAsync(int item, StreamSequenceToken? token = null)
{
    Console.WriteLine("OnNextAsync: item: {0}, token = {1}", item, token);
    return Task.CompletedTask;
}

async Task<bool> RetryFilter(Exception exception, CancellationToken cancellationToken)
{
    attempt++;
    Console.WriteLine($"Cluster client attempt {attempt} of {maxAttempts} failed to connect to cluster.  Exception: {exception}");
    if (attempt > maxAttempts)
        return false;

    await Task.Delay(TimeSpan.FromSeconds(4), cancellationToken);
    return true;
}

//internal class Program
//{
//    private static async Task<int> Main(string[] args)
//    {
//        const int maxAttempts = 5;
//        var attempt = 0;

//        try
//        {
//            var host = new HostBuilder()
//                .ConfigureLogging(logging => logging.AddConsole())
//                .UseOrleansClient((context, client) =>
//                {
//                    client
//                        .UseLocalhostClustering(serviceId: Constants.ServiceId, clusterId: Constants.ClusterId)
//                        .UseConnectionRetryFilter(RetryFilter)
//                        .AddRabbitMqStreams(Constants.StreamProvider, optionsBuilder =>
//                        {
//                            optionsBuilder.Configure(options =>
//                            {
//                                options.ConnectionString = "amqp://guest:guest@localhost:5672/";
//                                options.ExchangeName = "orleans-test-exchange";
//                                options.QueueNamePrefix = "orleans-test-queue";
//                                options.DeclareQueue = true;
//                            });
//                        });
//                })
//                .Build();

//            await host.StartAsync();
//            Console.WriteLine("Client successfully connect to silo host");

//            var client = host.Services.GetRequiredService<IClusterClient>();

//            // Use the connected client to ask a grain to start producing events
//            var key = Guid.NewGuid();
//            var producer = client.GetGrain<IProducerGrain>("my-producer");
//            await producer.StartProducing(Constants.StreamNamespace, key);

//            // Now you should see that a consumer grain was activated on the silo, and is logging when it is receiving event

//            // Client can also subscribe to streams
//            var streamId = StreamId.Create(Constants.StreamNamespace, key);
//            var stream = client
//                .GetStreamProvider(Constants.StreamProvider)
//                .GetStream<int>(streamId);
//            await stream.SubscribeAsync(OnNextAsync);

//            // Now the client will also log received events

//            await Task.Delay(TimeSpan.FromSeconds(15));

//            // Stop producing
//            await producer.StopProducing();

//            Console.ReadKey();
//            return 0;
//        }
//        catch (Exception e)
//        {
//            Console.WriteLine(e);
//            Console.ReadKey();
//            return 1;
//        }

//        static Task OnNextAsync(int item, StreamSequenceToken? token = null)
//        {
//            Console.WriteLine("OnNextAsync: item: {0}, token = {1}", item, token);
//            return Task.CompletedTask;
//        }

//        async Task<bool> RetryFilter(Exception exception, CancellationToken cancellationToken)
//        {
//            attempt++;
//            Console.WriteLine($"Cluster client attempt {attempt} of {maxAttempts} failed to connect to cluster.  Exception: {exception}");
//            if (attempt > maxAttempts)
//            {
//                return false;
//            }
//            await Task.Delay(TimeSpan.FromSeconds(4), cancellationToken);
//            return true;
//        }
//    }
//}