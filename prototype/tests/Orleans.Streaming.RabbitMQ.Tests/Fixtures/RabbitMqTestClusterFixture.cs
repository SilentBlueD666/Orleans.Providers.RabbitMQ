using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Streaming.RabbitMQ.Tests.Fixtures;
using Orleans.TestingHost;

namespace Orleans.Streaming.RabbitMQ.Tests.Fixtures;

public sealed class RabbitMqTestClusterFixture : BaseTestClusterFixture
{
    protected override void ConfigureTestCluster(TestClusterBuilder builder)
    {
        builder.AddSiloBuilderConfigurator<RabbitMqSiloConfigurator>();
    }

    private sealed class RabbitMqSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
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
                    options.ConnectionString = "amqp://guest:guest@localhost:5672/";
                    options.ExchangeName = "orleans-test-exchange";
                    options.QueueNamePrefix = "orleans-test-queue";
                    options.DeclareQueue = true;
                });
            });
        }
    }
}