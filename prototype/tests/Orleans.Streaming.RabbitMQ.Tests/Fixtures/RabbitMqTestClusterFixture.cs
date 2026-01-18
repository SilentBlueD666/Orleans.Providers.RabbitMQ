using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Streaming.RabbitMQ.Tests.Fixtures;
using Orleans.TestingHost;
using Testcontainers.RabbitMq;

namespace Orleans.Streaming.RabbitMQ.Tests.Fixtures;

public sealed class RabbitMqTestClusterFixture : BaseTestClusterFixture
{
    private const int RabbitMqPort = 5672;
    private static RabbitMqContainer? _container;
    private static string? _connectionString;

    public override async Task InitializeAsync()
    {
        _container = new RabbitMqBuilder("rabbitmq:4.2.2-management")
            .WithLabel("test", "orleans-rabbitmq")
            .WithPortBinding(RabbitMqPort, true)
            .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
            .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
            .Build();

        await _container.StartAsync();
        
        _connectionString = $"amqp://guest:guest@localhost:{_container.GetMappedPublicPort(RabbitMqPort)}/";

        await base.InitializeAsync();
    }

    public override async Task DisposeAsync()
    {
        try
        {
            await base.DisposeAsync();
        }
        finally
        {
            if (_container is not null)
            {
                await _container.StopAsync();
                await _container.DisposeAsync();
                _container = null;
                _connectionString = null;
            }
        }
    }

    protected override void ConfigureTestCluster(TestClusterBuilder builder)
    {
        builder.AddSiloBuilderConfigurator<RabbitMqSiloConfigurator>();
    }

    private sealed class RabbitMqSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            var connectionString = _connectionString ?? throw new InvalidOperationException("RabbitMQ container connection string is not initialized");

            siloBuilder.ConfigureLogging(logging =>
            {
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Trace);
                logging.AddFilter("Orleans", LogLevel.Information);
            });

            siloBuilder.AddRabbitMq(TestConstants.StreamProvider, optionsBuilder =>
            {
                optionsBuilder.Configure(options =>
                {
                    options.ConnectionString = connectionString;
                    options.ExchangeName = "orleans-test-exchange";
                    options.QueueNamePrefix = "orleans-test-queue";
                });
            });
        }
    }
}