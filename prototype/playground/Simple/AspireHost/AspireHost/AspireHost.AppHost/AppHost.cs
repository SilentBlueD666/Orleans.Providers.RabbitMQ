using Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("redis");

var orleans = builder.AddOrleans("my-app")
    .WithClusterId("dev")
    .WithServiceId("simple-streaming")
    .WithClustering(redis)
    .WithMemoryGrainStorage("Default");

builder.AddProject<Projects.SiloHost>("silohost")
    .WithReference(redis)
    .WithReference(orleans);

builder.AddProject<Projects.Client>("client")
    .WithReference(redis)
    .WithReference(orleans.AsClient());

builder.Build().Run();
