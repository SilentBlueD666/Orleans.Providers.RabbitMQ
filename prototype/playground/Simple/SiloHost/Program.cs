using Common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddKeyedRedisClient("redis");

builder.Logging
    .AddConsole()
    .SetMinimumLevel(LogLevel.Debug)
    .AddFilter("Orleans", LogLevel.Information);

builder.UseOrleans(siloBuilder
    => siloBuilder
        .AddRabbitMq(Constants.StreamProvider, ob =>
        {
            ob.Configure(options =>
            {
                options.ConnectionString = "amqp://guest:guest@localhost:5672/";
                options.ExchangeName = "orleans-test-exchange";
                options.QueueNamePrefix = "orleans-test-queue";
                options.DeclareQueue = true;
                options.ConnectionNamePrefix = "Orleans.Streaming.Silo.RabbitMQ";
            });
        }));

var app = builder.Build();

app.MapGet("/", () => "OK");

await app.RunAsync();