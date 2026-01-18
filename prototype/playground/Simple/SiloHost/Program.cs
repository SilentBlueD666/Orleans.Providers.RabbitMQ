using GrainInterfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Hosting;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddKeyedRedisClient("redis");

builder.Logging
    .AddConsole()
    .SetMinimumLevel(LogLevel.Debug)
    .AddFilter("Orleans", LogLevel.Information);

var rabbitMqConnectionString = builder.Configuration.GetConnectionString("rabbitmq")
    ?? throw new InvalidOperationException("RabbitMQ connection string is not configured.");

builder.UseOrleans(siloBuilder
    => siloBuilder.AddRabbitMq(Constants.StreamProvider, rabbitMqConnectionString));

var app = builder.Build();

app.MapGet("/", () => "OK");

await app.RunAsync();