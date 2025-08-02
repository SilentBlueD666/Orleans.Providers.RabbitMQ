using Orleans.TestingHost;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ.Tests.Fixtures;

internal sealed class TestDefaultConfiguration
{
    public static void InitializeDefaults()
    {
    }

    public static void ConfigureTestCluster(TestClusterBuilder builder)
    {
        // poor fault injection requires grain instances stay on same host, so only single host for this test
        builder.Options.InitialSilosCount = 1;
        builder.Options.ClusterId = "TestCluster";
        builder.Options.ServiceId = "TestService";
    }
}
