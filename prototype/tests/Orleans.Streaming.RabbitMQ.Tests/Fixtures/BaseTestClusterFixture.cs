using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.TestingHost;
using Xunit.Abstractions;

namespace Orleans.Streaming.RabbitMQ.Tests.Fixtures;

/// <summary>
/// Base class for test fixtures that host an Orleans cluster.
/// </summary>
public abstract class BaseTestClusterFixture : IAsyncLifetime
{
    private TestCluster? _hostedCluster;
    private ILogger? _logger;

    public TestCluster HostedCluster
    {
        get => _hostedCluster ?? throw new InvalidOperationException("Cluster has not been initialized. Call InitializeAsync first.");
        private set => _hostedCluster = value;
    }

    public IServiceProvider ServiceProvider => this.HostedCluster?.ServiceProvider ?? throw new InvalidOperationException("Cluster has not been initialized. Call InitializeAsync first.");

    public IGrainFactory GrainFactory => this.HostedCluster?.GrainFactory ?? throw new InvalidOperationException("Cluster has not been initialized. Call InitializeAsync first.");

    public IClusterClient Client => this.HostedCluster?.Client ?? throw new InvalidOperationException("Cluster has not been initialized. Call InitializeAsync first.");

    public ILoggerFactory LoggerFactory => ServiceProvider.GetRequiredService<ILoggerFactory>();

    public ILogger Logger
    {
        get => _logger ?? throw new InvalidOperationException("Logger has not been initialized. Call InitializeAsync first.");
        private set => _logger = value;
    }

    public string GetClientServiceId() => Client?.ServiceProvider.GetRequiredService<IOptions<ClusterOptions>>().Value.ServiceId ?? throw new InvalidOperationException("Cluster has not been initialized. Call InitializeAsync first.");

    /// <summary>
    /// Configures the test cluster.
    /// </summary>
    /// <param name="builder"></param>
    protected virtual void ConfigureTestCluster(TestClusterBuilder builder) { }

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        TestDefaultConfiguration.ConfigureTestCluster(builder);
        this.ConfigureTestCluster(builder);

        var testCluster = builder.Build();
        if (testCluster.Primary is null)
            await testCluster.DeployAsync().ConfigureAwait(false);

        this.HostedCluster = testCluster;
        this.Logger = this.LoggerFactory.CreateLogger("Application");
    }

    public async Task DisposeAsync()
    {
        var cluster = this.HostedCluster;
        if (cluster is null) return;

        try
        {
            await cluster.StopAllSilosAsync().ConfigureAwait(false);
        }
        finally
        {
            await cluster.DisposeAsync().ConfigureAwait(false);
        }
    }
}
