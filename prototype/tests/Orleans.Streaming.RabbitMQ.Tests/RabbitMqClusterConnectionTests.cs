using Orleans.Streaming.RabbitMQ.Tests.Fixtures;
using Orleans.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace Orleans.Streaming.RabbitMQ.Tests;

public class RabbitMqClusterConnectionTests : IClassFixture<RabbitMqTestClusterFixture>
{
    private readonly RabbitMqTestClusterFixture _fixture;

    public RabbitMqClusterConnectionTests(RabbitMqTestClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void SiloIsConfiguredWithRabbitMqProvider()
    {
        // Arrange
        var serviceProvider = _fixture.HostedCluster.ServiceProvider;
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsSnapshot<RabbitMqOptions>>().Get("RabbitMQ");

        // Assert
        Assert.NotNull(options);
        Assert.False(string.IsNullOrWhiteSpace(options.ConnectionString));
        Assert.Equal("amqp://guest:guest@localhost:5672/", options.ConnectionString);
    }

    [Fact]
    public async Task CanEstablishRabbitMqConnection()
    {
        // Arrange
        var serviceProvider = _fixture.HostedCluster.ServiceProvider;
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsSnapshot<RabbitMqOptions>>().Get("RabbitMQ");

        // Act
        var factory = new ConnectionFactory
        {
            Uri = new Uri(options.ConnectionString!)
        };

        await using var connection = await factory.CreateConnectionAsync();

        //Assert
        Assert.True(connection.IsOpen);
    }
}