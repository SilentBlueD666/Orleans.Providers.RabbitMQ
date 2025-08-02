using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.TestingHost;

namespace Orleans.Streaming.RabbitMQ.Tests.Fixtures;

//public sealed class DefaultClusterFixture : BaseTestClusterFixture
//{
//    static DefaultClusterFixture()
//    {
//        TestDefaultConfiguration.InitializeDefaults();
//    }

//    protected override void ConfigureTestCluster(TestClusterBuilder builder)
//    {
//        builder.AddSiloBuilderConfigurator<SiloHostConfigurator>();
//    }

//    public class SiloHostConfigurator : ISiloConfigurator
//    {
//        public void Configure(ISiloBuilder hostBuilder)
//        {
//            hostBuilder
//                .Configure<SiloMessagingOptions>(o => o.ClientGatewayShutdownNotificationTimeout = default)
//                .UseInMemoryReminderService()
//                .AddMemoryGrainStorageAsDefault()
//                .AddMemoryGrainStorage("MemoryStore");
//        }
//    }
//}